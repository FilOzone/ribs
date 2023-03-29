package web

import (
	"context"

	"github.com/filecoin-project/go-jsonrpc"

	"github.com/lotus-web3/ribs"
)

type RIBSRpc struct {
	ribs ribs.RIBS
}

func (rc *RIBSRpc) WalletInfo(ctx context.Context) (ribs.WalletInfo, error) {
	return rc.ribs.Diagnostics().WalletInfo()
}

func (rc *RIBSRpc) Groups(ctx context.Context) ([]ribs.GroupKey, error) {
	return rc.ribs.Diagnostics().Groups()
}

func (rc *RIBSRpc) GroupMeta(ctx context.Context, group ribs.GroupKey) (ribs.GroupMeta, error) {
	return rc.ribs.Diagnostics().GroupMeta(group)
}

func (rc *RIBSRpc) CrawlState(ctx context.Context) (ribs.CrawlState, error) {
	return rc.ribs.Diagnostics().CrawlState(), nil
}

func (rc *RIBSRpc) CarUploadStats(ctx context.Context) (map[ribs.GroupKey]*ribs.UploadStats, error) {
	return rc.ribs.Diagnostics().CarUploadStats(), nil
}

func (rc *RIBSRpc) ReachableProviders(ctx context.Context) ([]ribs.ProviderMeta, error) {
	return rc.ribs.Diagnostics().ReachableProviders(), nil
}

func (rc *RIBSRpc) DealSummary(ctx context.Context) (ribs.DealSummary, error) {
	return rc.ribs.Diagnostics().DealSummary()
}

func (rc *RIBSRpc) TopIndexStats(ctx context.Context) (ribs.TopIndexStats, error) {
	return rc.ribs.Diagnostics().TopIndexStats(ctx)
}

func (rc *RIBSRpc) GroupIOStats(ctx context.Context) (ribs.GroupIOStats, error) {
	return rc.ribs.Diagnostics().GroupIOStats(), nil
}

func MakeRPCServer(ctx context.Context, ribs ribs.RIBS) (*jsonrpc.RPCServer, jsonrpc.ClientCloser, error) {
	hnd := &RIBSRpc{ribs: ribs}

	fgw, closer, err := ribs.Diagnostics().Filecoin(ctx)
	if err != nil {
		return nil, nil, err
	}

	sv := jsonrpc.NewServer()
	sv.Register("RIBS", hnd)
	sv.Register("Filecoin", fgw)

	return sv, closer, nil
}
