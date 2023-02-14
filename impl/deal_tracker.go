package impl

import (
	"context"
	"fmt"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"golang.org/x/xerrors"
	"time"
)

const DealStatusV12ProtocolID = "/fil/storage/status/1.2.0"
const clientReadDeadline = 10 * time.Second
const clientWriteDeadline = 10 * time.Second

var DealCheckInterval = 10 * time.Second

func (r *ribs) dealTracker(ctx context.Context) {
	gw, closer, err := client.NewGatewayRPCV1(ctx, "http://api.chain.love/rpc/v1", nil)
	if err != nil {
		panic(err)
	}
	defer closer()

	for {
		checkStart := time.Now()
		select {
		case <-r.close:
			return
		default:
		}

		err := r.runDealCheckLoop(ctx, gw)
		if err != nil {
			panic(err)
		}

		checkDuration := time.Since(checkStart)
		if checkDuration < DealCheckInterval {
			select {
			case <-r.close:
				return
			case <-time.After(DealCheckInterval - checkDuration):
			}
		}
	}
}

func (r *ribs) runDealCheckLoop(ctx context.Context, gw api.Gateway) error {
	toCheck, err := r.db.InactiveDeals()
	if err != nil {
		return xerrors.Errorf("get inactive deals: %w", err)
	}

	walletAddr, err := r.wallet.GetDefault()
	if err != nil {
		return xerrors.Errorf("get wallet address: %w", err)
	}

	for _, deal := range toCheck {
		maddr, err := address.NewIDAddress(uint64(deal.ProviderAddr))
		if err != nil {
			return xerrors.Errorf("new id address: %w", err)
		}

		dealUUID, err := uuid.Parse(deal.DealUUID)
		if err != nil {
			return xerrors.Errorf("parse deal uuid: %w", err)
		}

		addrInfo, err := GetAddrInfo(ctx, gw, maddr)
		if err != nil {
			return xerrors.Errorf("get addr info: %w", err)
		}

		if err := r.host.Connect(ctx, *addrInfo); err != nil {
			return xerrors.Errorf("connect to miner: %w", err)
		}

		resp, err := r.SendDealStatusRequest(ctx, addrInfo.ID, dealUUID, walletAddr)
		if err != nil {
			return fmt.Errorf("send deal status request failed: %w", err)
		}

		if err := r.db.UpdateSPDealState(dealUUID, *resp); err != nil {
			return xerrors.Errorf("storing deal state response: %w", err)
		}
	}

	return nil
}

func (r *ribs) SendDealStatusRequest(ctx context.Context, id peer.ID, dealUUID uuid.UUID, caddr address.Address) (*types.DealStatusResponse, error) {
	log.Debugw("send deal status req", "deal-uuid", dealUUID, "id", id)

	uuidBytes, err := dealUUID.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("getting uuid bytes: %w", err)
	}

	sig, err := r.wallet.WalletSign(ctx, caddr, uuidBytes, api.MsgMeta{Type: api.MTDealProposal})
	if err != nil {
		return nil, fmt.Errorf("signing uuid bytes: %w", err)
	}

	// Create a libp2p stream to the provider
	s, err := shared.NewRetryStream(r.host).OpenStream(ctx, id, []protocol.ID{DealStatusV12ProtocolID})
	if err != nil {
		return nil, err
	}

	defer s.Close() // nolint

	// Set a deadline on writing to the stream so it doesn't hang
	_ = s.SetWriteDeadline(time.Now().Add(clientWriteDeadline))
	defer s.SetWriteDeadline(time.Time{}) // nolint

	// Write the deal status request to the stream
	req := types.DealStatusRequest{DealUUID: dealUUID, Signature: *sig}
	if err = cborutil.WriteCborRPC(s, &req); err != nil {
		return nil, fmt.Errorf("sending deal status req: %w", err)
	}

	// Set a deadline on reading from the stream so it doesn't hang
	_ = s.SetReadDeadline(time.Now().Add(clientReadDeadline))
	defer s.SetReadDeadline(time.Time{}) // nolint

	// Read the response from the stream
	var resp types.DealStatusResponse
	if err := resp.UnmarshalCBOR(s); err != nil {
		return nil, fmt.Errorf("reading deal status response: %w", err)
	}

	log.Debugw("received deal status response", "id", resp.DealUUID, "status", resp.DealStatus)

	return &resp, nil
}