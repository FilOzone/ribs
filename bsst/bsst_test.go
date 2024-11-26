package bsst

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

type testSource struct {
	n int64
}

func (t *testSource) List(f func(c mh.Multihash, offs []int64) error) error {
	for i := int64(0); i < t.n; i++ {
		var ib [8]byte
		binary.LittleEndian.PutUint64(ib[:], uint64(i))

		h, err := mh.Sum(ib[:], mh.SHA2_256, -1)
		if err != nil {
			return err
		}

		if err := f(h, []int64{i | 0x7faa_0000_c000_0000}); err != nil {
			return err
		}
	}

	return nil
}

var _ Source = (*testSource)(nil)

func TestBSSTCreate(t *testing.T) {
	n := int64(13_421_280)

	bsst, err := Create(filepath.Join(t.TempDir(), "/a.bsst"), n, &testSource{n: n})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bsst.Close()) })

	var got int64

	err = (&testSource{n: n}).List(func(c mh.Multihash, offs []int64) error {
		h, err := bsst.Has([]mh.Multihash{c})
		require.NoError(t, err)
		if !h[0] {
			require.True(t, h[0]) // todo probably full buckets
		}

		r, err := bsst.Get([]mh.Multihash{c})
		require.NoError(t, err)
		require.Equal(t, offs[0], r[0])

		got++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, n, got)
}

func TestBSSTCreate2(t *testing.T) {
	n := int64(13_421_280)

	bsst, err := Create2(filepath.Join(t.TempDir(), "/a.bsst"), n, &testSource{n: n})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bsst.Close()) })

	var got int64

	err = (&testSource{n: n}).List(func(c mh.Multihash, offs []int64) error {
		h, err := bsst.Has([]mh.Multihash{c})
		require.NoError(t, err)
		if !h[0] {
			require.True(t, h[0]) // todo probably full buckets
		}

		r, err := bsst.Get([]mh.Multihash{c})
		require.NoError(t, err)
		require.Equal(t, offs[0], r[0])

		got++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, n, got)
}
