package rbstor

import (
	"context"
	"database/sql"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	iface "github.com/lotus-web3/ribs"
	_ "github.com/mattn/go-sqlite3"
	mh "github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

var log = logging.Logger("rbs")

type openOptions struct {
	db *sql.DB
}

type OpenOption func(*openOptions)

func WithDB(db *sql.DB) OpenOption {
	return func(o *openOptions) {
		o.db = db
	}
}

const workerCount = 8

// todo root as option, separate data / data index / index (/ staging?) paths
func Open(root string, opts ...OpenOption) (iface.RBS, error) {
	if err := os.Mkdir(root, 0755); err != nil && !os.IsExist(err) {
		return nil, xerrors.Errorf("make root dir: %w", err)
	}

	idx, err := NewPebbleIndex(filepath.Join(root, "index.pebble"))
	if err != nil {
		return nil, xerrors.Errorf("open top index: %w", err)
	}

	opt := &openOptions{}

	for _, o := range opts {
		o(opt)
	}

	db, err := openRibsDB(root, opt.db)
	if err != nil {
		return nil, xerrors.Errorf("open db: %w", err)
	}

	r := &rbs{
		root:  root,
		db:    db,
		index: NewMeteredIndex(idx),

		writableGroups: make(map[iface.GroupKey]*Group),

		// all open groups (including all writable)
		openGroups: make(map[iface.GroupKey]*Group),

		tasks: make(chan task, 1024),

		close: make(chan struct{}),
	}

	for i := 0; i < workerCount; i++ {
		r.workerClosed[i] = make(chan struct{})
	}

	return r, nil
}

func (r *rbs) Start() error {
	for i := 0; i < workerCount; i++ {
		go r.groupWorker(i)
	}
	go r.resumeGroups(context.TODO())

	return nil
}

type taskType int

const (
	taskTypeFinalize taskType = iota
	taskTypeGenCommP
)

type task struct {
	tt    taskType
	group iface.GroupKey
}

type rbs struct {
	root string

	// todo hide this db behind an interface
	db    *rbsDB
	index *MeteredIndex

	lk      sync.Mutex
	writeLk sync.Mutex

	/* subs */
	subLk sync.Mutex
	subs  []iface.GroupSub

	/* storage */

	close        chan struct{}
	workerClosed [workerCount]chan struct{}

	tasks chan task

	openGroups     map[int64]*Group
	writableGroups map[int64]*Group

	external atomic.Pointer[iface.ExternalStorageProvider]
	staging  atomic.Pointer[iface.StagingStorageProvider]

	// diag cache
	diagLk sync.Mutex

	grpReadBlocks  int64
	grpReadSize    int64
	grpWriteBlocks int64
	grpWriteSize   int64
}

func (r *rbs) Close() error {
	close(r.close)
	for i := 0; i < workerCount; i++ {
		<-r.workerClosed[i]
	}

	r.lk.Lock()
	defer r.lk.Unlock()

	for _, g := range r.openGroups {
		if err := g.Close(); err != nil {
			return xerrors.Errorf("closing group %d: %w", g.id, err)
		}
	}

	if err := r.index.Close(); err != nil {
		return xerrors.Errorf("closing index: %w", err)
	}

	log.Errorf("TODO mark closed")

	return nil
}

type ribSession struct {
	r *rbs
}

type ribBatch struct {
	r *rbs

	currentWriteTarget iface.GroupKey
	toFlush            map[iface.GroupKey]struct{}

	// todo: use lru
	currentReadTarget iface.GroupKey
}

func (r *rbs) Session(ctx context.Context) iface.Session {
	return &ribSession{
		r: r,
	}
}

func (r *ribSession) View(ctx context.Context, c []mh.Multihash, cb func(cidx int, data []byte)) error {
	done := map[int]struct{}{}
	byGroup := map[iface.GroupKey][]int{}

	err := r.r.index.GetGroups(ctx, c, func(i [][]iface.GroupKey) (bool, error) {
		for cidx, groups := range i {
			if _, ok := done[cidx]; ok {
				continue
			}
			done[cidx] = struct{}{}

			for _, g := range groups {
				if g == iface.UndefGroupKey {
					continue
				}

				byGroup[g] = append(byGroup[g], cidx)
			}
		}

		return len(done) != len(c), nil
	})
	if err != nil {
		return err
	}

	for g, cidxs := range byGroup {
		toGet := make([]mh.Multihash, len(cidxs))
		for i, cidx := range cidxs {
			toGet[i] = c[cidx]
		}

		err := r.r.withReadableGroup(ctx, g, func(g *Group) error {
			return g.View(ctx, toGet, func(cidx int, found bool, data []byte) {
				if !found {
					c := cid.NewCidV1(cid.Raw, toGet[cidx])
					log.Errorw("group: block not found", "mh", toGet[cidx], "cid", c.String(), "group", g.id)
					return
				}

				cb(cidxs[cidx], data)
			})
		})
		if err == ErrOffloaded {
			extp := r.r.external.Load()
			if extp == nil {
				return xerrors.Errorf("no external storage, group %d is offloaded", g)
			}

			ext := *extp
			return ext.FetchBlocks(ctx, g, toGet, func(cidx int, data []byte) {
				cb(cidxs[cidx], data)
			})
		} else if err != nil {
			return xerrors.Errorf("with readable group(%d)/view: %w", g, err)
		}
	}

	return nil
}

func (r *ribSession) GetSize(ctx context.Context, c []mh.Multihash, cb func(i []int32) error) error {
	return r.r.index.GetSizes(ctx, c, cb)
}

func (r *ribSession) Batch(ctx context.Context) iface.Batch {
	return &ribBatch{
		r:                  r.r,
		currentWriteTarget: iface.UndefGroupKey,
		toFlush:            map[iface.GroupKey]struct{}{},
	}
}

func (r *ribBatch) Put(ctx context.Context, b []blocks.Block) error {
	// todo filter blocks that already exist
	var done int
	for done < len(b) {
		gk, err := r.r.withWritableGroup(ctx, r.currentWriteTarget, func(g *Group) error {
			wrote, err := g.Put(ctx, b[done:])
			if err != nil {
				return err
			}
			done += wrote
			return nil
		})
		if err != nil {
			return xerrors.Errorf("write to group: %w", err)
		}

		r.toFlush[gk] = struct{}{}
		r.currentWriteTarget = gk
	}

	return nil
}

func (r *ribBatch) Unlink(ctx context.Context, c []mh.Multihash) error {
	//TODO implement me
	panic("implement me")
}

func (r *ribBatch) Flush(ctx context.Context) error {
	r.r.lk.Lock()
	defer r.r.lk.Unlock()

	for key := range r.toFlush { // todo run in parallel
		g, found := r.r.writableGroups[key]
		if !found {
			continue // already flushed
		}
		r.r.lk.Unlock()
		err := g.Sync(ctx)
		r.r.lk.Lock()
		if err != nil {
			return xerrors.Errorf("sync group %d: %w", key, err)
		}
	}

	if err := r.r.index.Sync(ctx); err != nil {
		return xerrors.Errorf("flush top index: %w", err)
	}

	r.toFlush = map[iface.GroupKey]struct{}{}

	return nil
}

func (r *rbs) Offload(ctx context.Context, group iface.GroupKey) error {
	return r.withReadableGroup(ctx, group, func(g *Group) error {
		err := g.offload()
		return err
	})
}

func (r *rbs) FindHashes(ctx context.Context, hash mh.Multihash) ([]iface.GroupKey, error) {
	var out []iface.GroupKey

	err := r.index.GetGroups(ctx, []mh.Multihash{hash}, func(i [][]iface.GroupKey) (bool, error) {
		for _, groups := range i {
			for _, g := range groups {
				if g == iface.UndefGroupKey {
					continue
				}
				out = append(out, g)
			}
		}

		return true, nil
	})

	if err != nil {
		return nil, err
	}

	return out, nil
}

func (r *rbs) DescibeGroup(ctx context.Context, group iface.GroupKey) (iface.GroupDesc, error) {
	return r.db.DescibeGroup(ctx, group)
}

func (r *rbs) ReadCar(ctx context.Context, group iface.GroupKey, sz func(int64), out io.Writer) error {
	gm, err := r.db.GroupMeta(group)
	if err != nil {
		return xerrors.Errorf("getting group meta: %w", err)
	}
	if gm.DealCarSize == nil {
		return xerrors.Errorf("group has no deal car size set")
	}

	sz(*gm.DealCarSize)

	return r.withReadableGroup(ctx, group, func(g *Group) error {
		_, _, err := g.writeCar(out)
		return err
	})
}

func (r *rbs) HashSample(ctx context.Context, group iface.GroupKey) ([]mh.Multihash, error) {
	var out []mh.Multihash
	err := r.withReadableGroup(ctx, group, func(g *Group) error {
		var err error
		out, err = g.hashSample()
		return err
	})

	return out, err
}

func (r *rbs) Storage() iface.Storage {
	return r
}

func (r *rbs) ExternalStorage() iface.RBSExternalStorage {
	return r
}

func (r *rbs) InstallStagingProvider(provider iface.StagingStorageProvider) {
	r.staging.Store(&provider)
}

func (r *rbs) InstallProvider(provider iface.ExternalStorageProvider) {
	r.external.Store(&provider)
}

func (r *rbs) StagingStorage() iface.RBSStagingStorage {
	return r
}

var _ iface.RBS = &rbs{}
