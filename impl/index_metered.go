package impl

import (
	"context"
	iface "github.com/lotus-web3/ribs"
	"github.com/multiformats/go-multihash"
	"sync/atomic"
)

type MeteredIndex struct {
	sub iface.Index

	reads, writes int64
}

func (m *MeteredIndex) EstimateSize(ctx context.Context) (int64, error) {
	return m.sub.EstimateSize(ctx)
}

func (m *MeteredIndex) GetGroups(ctx context.Context, mh []multihash.Multihash, cb func([][]iface.GroupKey) (more bool, err error)) error {
	atomic.AddInt64(&m.reads, int64(len(mh)))
	return m.sub.GetGroups(ctx, mh, cb)
}

func (m *MeteredIndex) AddGroup(ctx context.Context, mh []multihash.Multihash, group iface.GroupKey) error {
	atomic.AddInt64(&m.writes, int64(len(mh)))
	return m.sub.AddGroup(ctx, mh, group)
}

func (m *MeteredIndex) Sync(ctx context.Context) error {
	return m.sub.Sync(ctx)
}

func (m *MeteredIndex) DropGroup(ctx context.Context, mh []multihash.Multihash, group iface.GroupKey) error {
	atomic.AddInt64(&m.writes, int64(len(mh)))
	return m.sub.DropGroup(ctx, mh, group)
}

func NewMeteredIndex(sub iface.Index) *MeteredIndex {
	return &MeteredIndex{sub: sub}
}

var _ iface.Index = &MeteredIndex{}