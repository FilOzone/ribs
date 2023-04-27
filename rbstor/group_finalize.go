package rbstor

import (
	"context"
	commcid "github.com/filecoin-project/go-fil-commcid"
	"github.com/ipfs/go-cid"
	iface "github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/carlog"
	"github.com/lotus-web3/ribs/ributil"
	"golang.org/x/xerrors"
	"time"
)

func (m *Group) Finalize(ctx context.Context) error {
	m.jblk.Lock()
	defer m.jblk.Unlock()

	if m.state != iface.GroupStateFull {
		return xerrors.Errorf("group not in state for finalization: %d", m.state)
	}

	if err := m.jb.MarkReadOnly(); err != nil && err != carlog.ErrReadOnly {
		return xerrors.Errorf("mark read-only: %w", err)
	}

	if err := m.jb.Finalize(); err != nil {
		return xerrors.Errorf("finalize jbob: %w", err)
	}

	if err := m.jb.DropLevel(); err != nil {
		return xerrors.Errorf("removing leveldb index: %w", err)
	}

	if err := m.advanceState(ctx, iface.GroupStateVRCARDone); err != nil {
		return xerrors.Errorf("mark level index dropped: %w", err)
	}

	return nil
}

func (m *Group) GenCommP() error {
	if m.state != iface.GroupStateVRCARDone {
		return xerrors.Errorf("group not in state for generating top CAR: %d", m.state)
	}

	cc := new(ributil.DataCidWriter)

	start := time.Now()

	carSize, root, err := m.writeCar(cc)
	if err != nil {
		return xerrors.Errorf("write car: %w", err)
	}

	sum, err := cc.Sum()
	if err != nil {
		panic(err)
	}

	log.Infow("generated commP", "duration", time.Since(start), "commP", sum.PieceCID, "pps", sum.PieceSize, "mbps", float64(carSize)/time.Since(start).Seconds()/1024/1024)

	p, _ := commcid.CIDToDataCommitmentV1(sum.PieceCID)

	if err := m.setCommP(context.Background(), iface.GroupStateLocalReadyForDeals, p, int64(sum.PieceSize), root, carSize); err != nil {
		return xerrors.Errorf("set commP: %w", err)
	}

	return nil
}

func (m *Group) advanceState(ctx context.Context, st iface.GroupState) error {
	m.dblk.Lock()
	defer m.dblk.Unlock()

	m.state = st

	// todo enter failed state on error
	return m.db.SetGroupState(ctx, m.id, st)
}

func (m *Group) setCommP(ctx context.Context, state iface.GroupState, commp []byte, paddedPieceSize int64, root cid.Cid, carSize int64) error {
	m.dblk.Lock()
	defer m.dblk.Unlock()

	m.state = state

	// todo enter failed state on error
	return m.db.SetCommP(ctx, m.id, state, commp, paddedPieceSize, root, carSize)
}