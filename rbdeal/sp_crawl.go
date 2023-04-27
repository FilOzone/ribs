package rbdeal

import (
	"context"
	"errors"
	"fmt"
	"github.com/filecoin-project/boost/retrievalmarket/lp2pimpl"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/actors/adt"
	"github.com/filecoin-project/lotus/chain/actors/builtin/market"
	"github.com/filecoin-project/lotus/chain/types"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	inet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	iface "github.com/lotus-web3/ribs"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/xerrors"
	"sync"
	"sync/atomic"
	"time"
)

const AskProtocolID = "/fil/storage/ask/1.1.0"

var (
	crawlInit           = "init"
	crawlLoadMarket     = "loading market actor"
	crawlMarketList     = "listing market participants"
	crawlStoreMarket    = "storing market participants"
	crawlQueryProviders = "querying providers"
	crawlIdle           = "idle"
)

var crawlFrequency = 30 * time.Minute

func (r *ribs) setCrawlState(state iface.CrawlState) {
	r.crawlState.Store(&state)
}

func (r *ribs) spCrawler() {
	r.setCrawlState(iface.CrawlState{State: crawlInit})

	defer close(r.spCrawlClosed)

	ctx := context.TODO()

	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		panic(err)
	}
	defer closer()

	pingP2P, err := libp2p.New()
	if err != nil {
		panic(err)
	}

	for {
		select {
		case <-r.close:
			return
		default:
		}

		err := r.spCrawlLoop(ctx, gw, pingP2P)
		if err != nil {
			log.Errorw("sp crawl loop", "err", err)
		}

		r.setCrawlState(iface.CrawlState{State: crawlIdle})

		select {
		case <-r.close:
			return
		case <-time.After(crawlFrequency):
		}
	}
}

func (r *ribs) spCrawlLoop(ctx context.Context, gw api.Gateway, pingP2P host.Host) error {
	boostTptClient := lp2pimpl.NewTransportsClient(pingP2P)

	r.setCrawlState(iface.CrawlState{State: crawlLoadMarket})

	head, err := gw.ChainHead(ctx)
	if err != nil {
		return xerrors.Errorf("getting chain head: %w", err)
	}

	// todo at finality

	mktAct, err := gw.StateGetActor(ctx, market.Address, head.Key())
	if err != nil {
		return xerrors.Errorf("getting market actor: %w", err)
	}

	stor := adt.WrapStore(ctx, cbor.NewCborStore(blockstore.NewAPIBlockstore(gw)))

	mact, err := market.Load(stor, mktAct)
	if err != nil {
		return xerrors.Errorf("loading market actor: %w", err)
	}

	bt, err := mact.LockedTable()
	if err != nil {
		return xerrors.Errorf("getting locked table: %w", err)
	}

	actors := make([]int64, 0)

	n := 0

	err = bt.ForEach(func(k address.Address, v abi.TokenAmount) error {
		i, err := address.IDFromAddress(k)
		if err != nil {
			return err
		}
		actors = append(actors, int64(i))

		// todo fileter out accounts

		n++
		if n%10 == 0 {
			r.setCrawlState(iface.CrawlState{State: crawlMarketList, At: int64(n)})
		}

		select {
		case <-r.close:
			return errors.New("stop")
		default:
		}

		return nil
	})
	if err != nil {
		select {
		case <-r.close:
			return nil
		default:
		}
		return nil
	}

	r.setCrawlState(iface.CrawlState{State: crawlStoreMarket})

	if err := r.db.UpsertMarketActors(actors); err != nil {
		return xerrors.Errorf("upserting market actors: %w", err)
	}

	r.setCrawlState(iface.CrawlState{State: crawlQueryProviders})

	const parallel = 128
	throttle := make(chan struct{}, parallel)
	const timeout = time.Second * 8

	var stlk sync.Mutex

	var started, reachable, boost, bitswap, http int64

	for n, actor := range actors {
		select {
		case <-r.close:
			return nil
		default:
		}

		throttle <- struct{}{}
		started++

		if started%10 == 0 {
			r.setCrawlState(iface.CrawlState{
				State:     crawlQueryProviders,
				At:        int64(n),
				Reachable: atomic.LoadInt64(&reachable),
				Total:     int64(len(actors)),
				Boost:     atomic.LoadInt64(&boost),
				BBswap:    atomic.LoadInt64(&bitswap),
				BHttp:     atomic.LoadInt64(&http),
			})
		}

		go func(actor int64) {
			defer func() {
				<-throttle
			}()

			var res providerResult
			var err error

			defer func() {
				stlk.Lock()
				defer stlk.Unlock()

				if err != nil {
					log.Debugw("error querying provider", "actor", actor, "err", err)
				}

				if err := r.db.UpdateProviderProtocols(actor, res); err != nil {
					log.Errorw("error updating provider", "actor", actor, "err", err)
				}
			}()

			maddr, err := address.NewIDAddress(uint64(actor))
			if err != nil {
				return
			}

			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			pi, err := GetAddrInfo(ctx, gw, maddr)
			if err != nil {
				return
			}

			if err := pingP2P.Connect(ctx, *pi); err != nil {
				return
			}

			res.PingOk = true
			atomic.AddInt64(&reachable, 1)

			boostTpt, err := boostTptClient.SendQuery(ctx, pi.ID)
			if err != nil {
				return
			}

			res.BoostDeals = true // todo this is technically not necesarily true, but for now it is good enough
			atomic.AddInt64(&boost, 1)

			for _, protocol := range boostTpt.Protocols {
				switch protocol.Name {
				case "libp2p":
				case "http":
					res.BoosterHttp = true
					atomic.AddInt64(&http, 1)
				case "bitswap":
					res.BoosterBitswap = true
					atomic.AddInt64(&bitswap, 1)
				default:
				}
			}

			s, err := pingP2P.NewStream(ctx, pi.ID, AskProtocolID)
			if err != nil {
				return
			}
			defer s.Close()

			var resp network.AskResponse

			askRequest := network.AskRequest{
				Miner: maddr,
			}

			if err := doRpc(ctx, s, &askRequest, &resp); err != nil {
				return
			}

			if err := r.db.UpdateProviderStorageAsk(actor, resp.Ask.Ask); err != nil {
				log.Errorw("error updating provider ask", "actor", actor, "err", err)
			}

		}(actor)
	}

	for i := 0; i < parallel; i++ {
		throttle <- struct{}{}
	}

	return nil
}

type providerResult struct {
	PingOk         bool
	BoostDeals     bool
	BoosterHttp    bool
	BoosterBitswap bool
}

func GetAddrInfo(ctx context.Context, api api.Gateway, maddr address.Address) (*peer.AddrInfo, error) {
	minfo, err := api.StateMinerInfo(ctx, maddr, types.EmptyTSK)
	if err != nil {
		return nil, err
	}
	if minfo.PeerId == nil {
		return nil, fmt.Errorf("storage provider %s has no peer ID set on-chain", maddr)
	}

	var maddrs []multiaddr.Multiaddr
	for _, mma := range minfo.Multiaddrs {
		ma, err := multiaddr.NewMultiaddrBytes(mma)
		if err != nil {
			return nil, fmt.Errorf("storage provider %s had invalid multiaddrs in their info: %w", maddr, err)
		}
		maddrs = append(maddrs, ma)
	}
	if len(maddrs) == 0 {
		return nil, fmt.Errorf("storage provider %s has no multiaddrs set on-chain", maddr)
	}

	return &peer.AddrInfo{
		ID:    *minfo.PeerId,
		Addrs: maddrs,
	}, nil
}

func doRpc(ctx context.Context, s inet.Stream, req interface{}, resp interface{}) error {
	errc := make(chan error)
	go func() {
		if err := cborutil.WriteCborRPC(s, req); err != nil {
			errc <- fmt.Errorf("failed to send request: %w", err)
			return
		}

		if err := cborutil.ReadCborRPC(s, resp); err != nil {
			errc <- fmt.Errorf("failed to read response: %w", err)
			return
		}

		errc <- nil
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}