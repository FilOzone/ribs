package impl

import (
	"context"
	"encoding/binary"
	"fmt"
	blocks "github.com/ipfs/go-block-format"
	iface "github.com/lotus_web3/ribs"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"io/fs"
	"path/filepath"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	td := t.TempDir()
	t.Cleanup(func() {
		if err := filepath.Walk(td, func(path string, info fs.FileInfo, err error) error {
			t.Log(path)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	ctx := context.Background()

	ri, err := Open(td)
	require.NoError(t, err)

	sess := ri.Session(ctx)

	wb := sess.Batch(ctx)

	b := blocks.NewBlock([]byte("hello world"))
	h := b.Cid().Hash()

	err = wb.Put(ctx, []multihash.Multihash{h}, [][]byte{b.RawData()})
	require.NoError(t, err)

	err = wb.Flush(ctx)
	require.NoError(t, err)

	err = sess.View(ctx, []multihash.Multihash{h}, func(i int, b []byte) {
		require.Equal(t, 0, i)
		require.Equal(t, b, []byte("hello world"))
	})
	require.NoError(t, err)

	require.NoError(t, ri.Close())
}

func TestFullGroup(t *testing.T) {
	td := t.TempDir()
	t.Cleanup(func() {
		if err := filepath.Walk(td, func(path string, info fs.FileInfo, err error) error {
			t.Log(path)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	ctx := context.Background()

	workerGate := make(chan struct{}, 1)

	ri, err := Open(td, WithWorkerGate(workerGate))
	require.NoError(t, err)

	sess := ri.Session(ctx)

	wb := sess.Batch(ctx)

	var h multihash.Multihash

	for i := 0; i < 500; i++ {
		var blk [200_000]byte
		binary.BigEndian.PutUint64(blk[:], uint64(i))

		b := blocks.NewBlock(blk[:])
		h = b.Cid().Hash()

		err = wb.Put(ctx, []multihash.Multihash{h}, [][]byte{b.RawData()})
		require.NoError(t, err)

		err = wb.Flush(ctx)
		require.NoError(t, err)
	}

	gs, err := ri.Diagnostics().GroupMeta(1)
	require.NoError(t, err)
	require.Equal(t, iface.GroupStateFull, gs.State)

	err = sess.View(ctx, []multihash.Multihash{h}, func(i int, b []byte) {
		require.Equal(t, 0, i)
		//require.Equal(t, b, []byte("hello world"))
	})
	require.NoError(t, err)

	workerGate <- struct{}{} // trigger a worker to run for one cycle

	require.Eventually(t, func() bool {
		gs, err := ri.Diagnostics().GroupMeta(1)
		require.NoError(t, err)
		fmt.Println(gs.State)
		return gs.State == iface.GroupStateLevelIndexDropped
	}, 10*time.Second, 40*time.Millisecond)

	workerGate <- struct{}{} // trigger a worker to allow processing close

	require.NoError(t, ri.Close())
}
