package bsst

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"math"
	"math/bits"
	"os"
	"sort"

	"github.com/minio/sha256-simd"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

// todo monte carlo those values for best reads factor

// parameters dedicated by hardware / math
const (
	BucketSize = 4096

	EntriesPerBucket = 128
	EntrySize        = BucketSize / EntriesPerBucket

	EntKeyBytes = 24 // with 24 bytes hash (192 bits, plenty enough to not get collisions), 8 for user offset

	BucketHeaderEntries = 1
	BucketUserEntries   = EntriesPerBucket - BucketHeaderEntries

	BucketBloomFilterSize    = EntrySize
	BucketBloomFilterEntries = BucketBloomFilterSize * 8

	MeanEntriesPerBucket = EntriesPerBucket/2 + EntriesPerBucket/4

	LevelFactor = 16384 // could be 24576, but we want to avoid deeper level misses
)

const BsstMagic = "BSST\x00\x00\x01\x00"

type Source interface {
	// List calls the callback with multihashes in sorted order.
	// todo require source to be MHH sorted
	List(func(c multihash.Multihash, offs []int64) error) error
}

/**
BSST file format:

bbst file: [bucketHdr: [header]\0...] [bucket0: [ent...][entHdr: bktHead]] [bucket1: ...

header: ["BSST\x00\x00\x01\x00"] {buckets: i64, bucketSize: i64, entries: i64, salt: [32]byte, levels: i64, levelFactor: i64, finalized: bool}:cbormap

bktHead: [bloom: [64]byte]

ent: [hash: [32]byte, off: le64]

*/

type BSSTHeader struct {
	L0Buckets  int64
	BucketSize int64
	Entries    int64
	Salt       [32]byte

	Levels      int64
	LevelFactor int64
	Finalized   bool
}

type BSST struct {
	f *os.File
	h *BSSTHeader

	CreateSample []multihash.Multihash
}

var BsstCIDSampleSize = 1024

func Create2(path string, entries int64, source Source) (bsst *BSST, err error) {
	var salt [32]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return nil, xerrors.Errorf("generate salt: %w", err)
	}

	header := &BSSTHeader{
		Salt: salt,
	}

	var nextLevel []multiHashHash

	createSample := make([]multihash.Multihash, 0, BsstCIDSampleSize) // TODO!!!

	err = source.List(func(c multihash.Multihash, offs []int64) error {
		if len(createSample) < BsstCIDSampleSize {
			// copy c data

			c := append([]byte(nil), c...)
			createSample = append(createSample, c)
		}
		for i, off := range offs {
			nextLevel = append(nextLevel, header.makeMHH(c, int64(i), off))
		}
		return nil
	})
	if err != nil {
		return nil, xerrors.Errorf("write buckets: %w", err)
	}

	// sort buckets

	// todo parallel merge sort
	sort.Slice(nextLevel, func(i, j int) bool {
		return bytes.Compare(nextLevel[i].mhh[:], nextLevel[j].mhh[:]) < 0
	})

	bsst, err = CreateAdv(path, entries, salt, func(i int64) []byte { // todo test
		var ent [EntrySize]byte
		copy(ent[:], nextLevel[i].mhh[:])
		binary.LittleEndian.PutUint64(ent[EntKeyBytes:], uint64(nextLevel[i].off))
		return ent[:]
	})
	if err != nil {
		return nil, xerrors.Errorf("create adv: %w", err)
	}

	bsst.CreateSample = createSample
	return bsst, nil
}

func CreateAdv(path string, entries int64, salt [32]byte, data func(i int64) []byte) (bsst *BSST, err error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, xerrors.Errorf("open file: %w", err)
	}

	header := &BSSTHeader{
		L0Buckets:  (entries + MeanEntriesPerBucket - 1) / MeanEntriesPerBucket, // ceil entries / MeanEntriesPerBucket
		BucketSize: BucketSize,
		Entries:    entries,

		Levels:      1,
		LevelFactor: LevelFactor,
		Finalized:   false,

		Salt: salt,
	}

	var hdrBuf [BucketSize]byte

	copy(hdrBuf[:], BsstMagic)

	if err := header.MarshalCBOR(bytes.NewBuffer(hdrBuf[8:])); err != nil {
		return nil, xerrors.Errorf("marshal header: %w", err)
	}

	if _, err := f.Write(hdrBuf[:]); err != nil {
		return nil, xerrors.Errorf("write header: %w", err)
	}

	// sync header
	if err := f.Sync(); err != nil {
		return nil, xerrors.Errorf("sync header: %w", err)
	}

	// write buckets
	bufWriter := bufio.NewWriterSize(f, 4<<20)
	nullEntry := [EntrySize]byte{}

	inBucket := uint64(0)
	bucketEnts := uint64(0)

	var bloomHead [BucketBloomFilterSize]byte

	flushBucket := func(bucketIdx uint64) error {
		if inBucket > bucketIdx {
			panic("buckets not sorted")
		}

		//bSizes[bucketEnts]++

		// pad up to BucketUserEntries entries
		for bucketEnts < BucketUserEntries {
			if _, err := bufWriter.Write(nullEntry[:]); err != nil {
				return xerrors.Errorf("write padding entry: %w", err)
			}
			bucketEnts++
		}

		// write bloom filter / header padding
		if _, err := bufWriter.Write(bloomHead[:]); err != nil {
			return xerrors.Errorf("write header entry filter: %w", err)
		}

		// reset temp vars
		inBucket++
		bucketEnts = 0

		return nil
	}

	var ents uint64
	var level int
	levelBuckets := uint64(header.L0Buckets)
	prevLevelBuckets := uint64(0)

	var nextLevel [][]byte
	first := true
	entsToIter := entries

	for len(nextLevel) > 0 || first {
		list := nextLevel
		nextLevel = [][]byte{}

		layerSource := data
		if !first {
			layerSource = func(i int64) []byte {
				return list[i]
			}
			entsToIter = int64(len(list))
		}

		first = false

		bucketRange := math.MaxUint64 / levelBuckets // todo techincally +1?? (if changing note this is also calculated below)

		for i := int64(0); i < entsToIter; i++ {
			entry := layerSource(i)

			// first 64 bits of hash to calculate bucket
			hashidx := binary.BigEndian.Uint64(entry[:8])
			bucketIdx := prevLevelBuckets + (hashidx / bucketRange)
			bloomEntIdx := (hashidx % bucketRange) / (bucketRange / BucketBloomFilterEntries)

			for inBucket != bucketIdx {
				if err := flushBucket(bucketIdx); err != nil {
					return nil, err
				}

				// reset bloom
				for i := range bloomHead[:BucketBloomFilterSize] {
					bloomHead[i] = 0
				}
			}

			// update bloom filter first, so that even if this entry won't fit in this level, we know that it doesn't
			// exist in one i/o if it's not in bloom
			bloomHead[bloomEntIdx/8] |= 1 << (bloomEntIdx % 8)

			// check if we have space for this entry
			if bucketEnts >= BucketUserEntries {
				nextLevel = append(nextLevel, entry)
				continue
			}

			// write entry
			if _, err := bufWriter.Write(entry[:EntrySize]); err != nil {
				return nil, xerrors.Errorf("write entry: %w", err)
			}

			bucketEnts++
			ents++
		}

		// flush last buckets
		for inBucket < levelBuckets+prevLevelBuckets { // todo is the condition right?
			if err := flushBucket(levelBuckets + prevLevelBuckets); err != nil {
				return nil, err
			}

			// todo could only do this once
			for i := range bloomHead[:BucketBloomFilterSize] {
				bloomHead[i] = 0
			}
		}

		level++
		prevLevelBuckets += levelBuckets
		levelBuckets = (levelBuckets + LevelFactor - 1) / LevelFactor // ceil(levelBuckets / LevelFactor)
	}

	header.Levels = int64(level)

	if err := bufWriter.Flush(); err != nil {
		return nil, xerrors.Errorf("flush buckets: %w", err)
	}

	if err := f.Sync(); err != nil {
		return nil, xerrors.Errorf("sync buckets: %w", err)
	}

	// finalize header
	header.Finalized = true
	if _, err := f.Seek(8, io.SeekStart); err != nil {
		return nil, xerrors.Errorf("seek to header: %w", err)
	}

	if err := header.MarshalCBOR(f); err != nil {
		return nil, xerrors.Errorf("marshal header: %w", err)
	}

	if err := f.Sync(); err != nil {
		return nil, xerrors.Errorf("sync header: %w", err)
	}

	// print bSizes sorted by key
	/*var keys []int
	for k := range bSizes {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)*/

	/*var ibe int64
	for _, k := range keys {
		ibe += bSizes[uint64(k)] * int64(k)
		//fmt.Printf("bs %d: %d\n", k, bSizes[uint64(k)])
	}*/

	//fmt.Println("ents ", ents, " bkt ", header.L0Buckets, " ibe ", ibe)

	if err := f.Close(); err != nil {
		return nil, xerrors.Errorf("close written bsst: %w", err)
	}

	// reopen in read-only mode
	bsst, err = Open(path)
	if err != nil {
		return nil, xerrors.Errorf("reopen bsst: %w", err)
	}

	return bsst, nil
}

func Create(path string, entries int64, source Source) (bsst *BSST, err error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, xerrors.Errorf("open file: %w", err)
	}

	header := &BSSTHeader{
		L0Buckets:  (entries + MeanEntriesPerBucket - 1) / MeanEntriesPerBucket, // ceil entries / MeanEntriesPerBucket
		BucketSize: BucketSize,
		Entries:    entries,

		Levels:      1,
		LevelFactor: LevelFactor,
		Finalized:   false,
	}
	if _, err := rand.Read(header.Salt[:]); err != nil {
		return nil, xerrors.Errorf("generate salt: %w", err)
	}

	var hdrBuf [BucketSize]byte

	copy(hdrBuf[:], BsstMagic)

	if err := header.MarshalCBOR(bytes.NewBuffer(hdrBuf[8:])); err != nil {
		return nil, xerrors.Errorf("marshal header: %w", err)
	}

	if _, err := f.Write(hdrBuf[:]); err != nil {
		return nil, xerrors.Errorf("write header: %w", err)
	}

	// sync header
	if err := f.Sync(); err != nil {
		return nil, xerrors.Errorf("sync header: %w", err)
	}

	// <todo mhh sorted source>

	// collect buckets
	// todo this in-memory stuff won't scale
	// could dump sorted to disk after some size, then merge (2x more writes, but eh..)
	var nextLevel []multiHashHash

	createSample := make([]multihash.Multihash, 0, BsstCIDSampleSize)

	err = source.List(func(c multihash.Multihash, offs []int64) error {
		if len(createSample) < BsstCIDSampleSize {
			// copy c data

			c := append([]byte(nil), c...)
			createSample = append(createSample, c)
		}
		for i, off := range offs {
			nextLevel = append(nextLevel, header.makeMHH(c, int64(i), off))
		}
		return nil
	})
	if err != nil {
		return nil, xerrors.Errorf("write buckets: %w", err)
	}

	// sort buckets

	// todo parallel merge sort
	sort.Slice(nextLevel, func(i, j int) bool {
		return bytes.Compare(nextLevel[i].mhh[:], nextLevel[j].mhh[:]) < 0
	})

	// </todo>

	// write buckets
	bufWriter := bufio.NewWriterSize(f, 4<<20)
	nullEntry := [EntrySize]byte{}
	var offBuf [EntrySize - EntKeyBytes]byte

	inBucket := uint64(0)
	bucketEnts := uint64(0)

	var bloomHead [BucketBloomFilterSize]byte

	//bSizes := map[uint64]int64{}

	flushBucket := func(bucketIdx uint64) error {
		if inBucket > bucketIdx {
			panic("buckets not sorted")
		}

		//bSizes[bucketEnts]++

		// pad up to BucketUserEntries entries
		for bucketEnts < BucketUserEntries {
			if _, err := bufWriter.Write(nullEntry[:]); err != nil {
				return xerrors.Errorf("write padding entry: %w", err)
			}
			bucketEnts++
		}

		// write bloom filter / header padding
		if _, err := bufWriter.Write(bloomHead[:]); err != nil {
			return xerrors.Errorf("write header entry filter: %w", err)
		}

		// reset temp vars
		inBucket++
		bucketEnts = 0

		return nil
	}

	var ents uint64
	var level int
	levelBuckets := uint64(header.L0Buckets)
	prevLevelBuckets := uint64(0)

	for len(nextLevel) > 0 {
		list := nextLevel
		nextLevel = []multiHashHash{}

		bucketRange := math.MaxUint64 / levelBuckets // todo techincally +1?? (if changing note this is also calculated below)

		for _, hash := range list {
			// first 64 bits of hash to calculate bucket
			hashidx := binary.BigEndian.Uint64(hash.mhh[:8])
			bucketIdx := prevLevelBuckets + (hashidx / bucketRange)
			bloomEntIdx := (hashidx % bucketRange) / (bucketRange / BucketBloomFilterEntries)

			for inBucket != bucketIdx {
				if err := flushBucket(bucketIdx); err != nil {
					return nil, err
				}

				// reset bloom
				for i := range bloomHead[:BucketBloomFilterSize] {
					bloomHead[i] = 0
				}
			}

			// update bloom filter first, so that even if this entry won't fit in this level, we know that it doesn't
			// exist in one i/o if it's not in bloom
			bloomHead[bloomEntIdx/8] |= 1 << (bloomEntIdx % 8)

			// check if we have space for this entry
			if bucketEnts >= BucketUserEntries {
				nextLevel = append(nextLevel, hash)
				continue
			}

			// write entry
			if _, err := bufWriter.Write(hash.mhh[:EntKeyBytes]); err != nil {
				return nil, xerrors.Errorf("write entry: %w", err)
			}
			binary.LittleEndian.PutUint64(offBuf[:], uint64(hash.off))
			if _, err := bufWriter.Write(offBuf[:]); err != nil {
				return nil, xerrors.Errorf("write entry offset: %w", err)
			}

			bucketEnts++
			ents++
		}

		// flush last buckets
		for inBucket < levelBuckets+prevLevelBuckets { // todo is the condition right?
			if err := flushBucket(levelBuckets + prevLevelBuckets); err != nil {
				return nil, err
			}

			// todo could only do this once
			for i := range bloomHead[:BucketBloomFilterSize] {
				bloomHead[i] = 0
			}
		}

		level++
		prevLevelBuckets += levelBuckets
		levelBuckets = (levelBuckets + LevelFactor - 1) / LevelFactor // ceil(levelBuckets / LevelFactor)
	}

	header.Levels = int64(level)

	if err := bufWriter.Flush(); err != nil {
		return nil, xerrors.Errorf("flush buckets: %w", err)
	}

	if err := f.Sync(); err != nil {
		return nil, xerrors.Errorf("sync buckets: %w", err)
	}

	// finalize header
	header.Finalized = true
	if _, err := f.Seek(8, io.SeekStart); err != nil {
		return nil, xerrors.Errorf("seek to header: %w", err)
	}

	if err := header.MarshalCBOR(f); err != nil {
		return nil, xerrors.Errorf("marshal header: %w", err)
	}

	if err := f.Sync(); err != nil {
		return nil, xerrors.Errorf("sync header: %w", err)
	}

	// print bSizes sorted by key
	/*var keys []int
	for k := range bSizes {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)*/

	/*var ibe int64
	for _, k := range keys {
		ibe += bSizes[uint64(k)] * int64(k)
		//fmt.Printf("bs %d: %d\n", k, bSizes[uint64(k)])
	}*/

	//fmt.Println("ents ", ents, " bkt ", header.L0Buckets, " ibe ", ibe)

	if err := f.Close(); err != nil {
		return nil, xerrors.Errorf("close written bsst: %w", err)
	}

	// reopen in read-only mode
	bsst, err = Open(path)
	if err != nil {
		return nil, xerrors.Errorf("reopen bsst: %w", err)
	}

	bsst.CreateSample = createSample

	return bsst, nil
}

func Open(path string) (*BSST, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, xerrors.Errorf("open file: %w", err)
	}

	var hdrBuf [BucketSize]byte
	if _, err := f.Read(hdrBuf[:]); err != nil {
		return nil, xerrors.Errorf("read header: %w", err)
	}

	if !bytes.Equal(hdrBuf[:8], []byte(BsstMagic)) {
		return nil, xerrors.Errorf("invalid magic")
	}

	var header BSSTHeader
	if err := header.UnmarshalCBOR(bytes.NewReader(hdrBuf[8:])); err != nil {
		return nil, xerrors.Errorf("unmarshal header: %w", err)
	}

	return &BSST{
		f: f,
		h: &header,
	}, nil
}

func bucketInd(k [32]byte, bucketRange, prevLevelBuckets uint64) (uint64, uint64) {
	hashidx := binary.BigEndian.Uint64(k[:8])
	bucketIdx := prevLevelBuckets + (hashidx / bucketRange)
	bloomEntIdx := (hashidx % bucketRange) / (bucketRange / BucketBloomFilterEntries)

	return bucketIdx, bloomEntIdx
}

type multiHashHash struct {
	mhh [32]byte
	off int64
}

func (h *BSSTHeader) makeMHH(c multihash.Multihash, i int64, off int64) multiHashHash {
	return multiHashHash{
		mhh: h.makeMHKey(c, i),
		off: off,
	}
}

func (h *BSSTHeader) makeMHKey(c multihash.Multihash, i int64) [32]byte {
	// buf = [salt][i: le64][c[:64]]
	var buf [(32 + 8) + (32 * 2)]byte
	copy(buf[:], h.Salt[:])
	binary.LittleEndian.PutUint64(buf[32:], uint64(i))
	copy(buf[32+8:], c)

	return sha256.Sum256(buf[:])
}

func (h *BSST) Has(c []multihash.Multihash) ([]bool, error) {
	keys := make([][32]byte, len(c))
	for i, k := range c {
		keys[i] = h.h.makeMHKey(k, 0) // todo support multiple results
	}

	out := make([]bool, len(c))

	var bucketBuf [BucketSize]byte

top:
	for i, k := range keys {
		levelBuckets := uint64(h.h.L0Buckets)
		prevLevelBuckets := uint64(0)

		for level := int64(0); level < h.h.Levels; level++ {
			bucketRange := math.MaxUint64 / levelBuckets
			bucketIdx, bloomEntIdx := bucketInd(k, bucketRange, prevLevelBuckets)

			if _, err := h.f.ReadAt(bucketBuf[:], int64(bucketIdx+1)*BucketSize); err != nil { // todo use mmap so we get page caching for free
				return nil, xerrors.Errorf("read bucket: %w", err)
			}

			// check if exists in bloom
			bloomOff := uint64(BucketUserEntries * EntrySize)
			if bucketBuf[bloomOff+bloomEntIdx/8]&(1<<(bloomEntIdx%8)) == 0 {
				// definitely not in bucket or next levels
				continue
			}

			// calculate minimum possible offset from bloom filter
			// note: this assumes 32byte bloom
			b0 := binary.LittleEndian.Uint64(bucketBuf[bloomOff+0 : bloomOff+8]) // LE because smallest byte is first
			b1 := binary.LittleEndian.Uint64(bucketBuf[bloomOff+8 : bloomOff+16])
			b2 := binary.LittleEndian.Uint64(bucketBuf[bloomOff+16 : bloomOff+24])
			b3 := binary.LittleEndian.Uint64(bucketBuf[bloomOff+24 : bloomOff+32])

			// now generate a mask that is bloomEntIdx bits long
			mLast := uint64(0xffffffffffffffff) >> (63 - (bloomEntIdx % 64))

			var inM1, inM2, inM3 uint64

			/*
				if bloomEntIdx > 63 {
					inM1 = 1
				}
				if bloomEntIdx > 127 {
					inM2 = 1
				}
				if bloomEntIdx > 191 {
					inM3 = 1
				}
			*/
			// bloomEntIdx is 0 <= x < 256

			/*
				inM0 = true
				inM1 = bei:b7 | bei:b6
				inM2 = bei:b7
				inM3 = bei:b7 & bei:b6
			*/

			bei6 := bloomEntIdx >> 6

			inM2 = (bloomEntIdx >> 7) & 1
			inM1 = inM2 | bei6
			inM3 = inM2 & bei6

			m0 := mLast | (-inM1)
			m1 := (mLast & (-inM1)) | (-inM2)
			m2 := (mLast & (-inM2)) | (-inM3)
			m3 := mLast & (-inM3)

			// count bits
			minOffIdx := (bits.OnesCount64(b0&m0) + bits.OnesCount64(b1&m1) + bits.OnesCount64(b2&m2) + bits.OnesCount64(b3&m3)) - 1
			for entIdx := minOffIdx; entIdx < BucketUserEntries; entIdx++ {
				if bytes.Equal(bucketBuf[entIdx*EntrySize:entIdx*EntrySize+EntKeyBytes], k[:EntKeyBytes]) {
					out[i] = true
					continue top
				}
			}

			prevLevelBuckets += levelBuckets
			levelBuckets = (levelBuckets + LevelFactor - 1) / LevelFactor
		}
	}

	return out, nil
}

// Get returns offsets to data, -1 if not found
func (h *BSST) Get(c []multihash.Multihash) ([]int64, error) {
	// todo dedupe code with Has

	keys := make([][32]byte, len(c))
	for i, k := range c {
		keys[i] = h.h.makeMHKey(k, 0) // todo support multiple results
	}

	out := make([]int64, len(c))

	var bucketBuf [BucketSize]byte

top:
	for i, k := range keys {
		out[i] = -1
		levelBuckets := uint64(h.h.L0Buckets)
		prevLevelBuckets := uint64(0)

		for level := int64(0); level < h.h.Levels; level++ {
			bucketRange := math.MaxUint64 / levelBuckets
			bucketIdx, bloomEntIdx := bucketInd(k, bucketRange, prevLevelBuckets)

			if _, err := h.f.ReadAt(bucketBuf[:], int64(bucketIdx+1)*BucketSize); err != nil { // todo use mmap so we get page caching for free
				return nil, xerrors.Errorf("read bucket: %w", err)
			}

			// check if exists in bloom
			bloomOff := uint64(BucketUserEntries * EntrySize)
			if bucketBuf[bloomOff+bloomEntIdx/8]&(1<<(bloomEntIdx%8)) == 0 {
				// definitely not in bucket
				continue
			}

			// calculate minimum possible offset from bloom filter
			// note: this assumes 32byte bloom
			b0 := binary.LittleEndian.Uint64(bucketBuf[bloomOff+0 : bloomOff+8]) // LE because smallest byte is first
			b1 := binary.LittleEndian.Uint64(bucketBuf[bloomOff+8 : bloomOff+16])
			b2 := binary.LittleEndian.Uint64(bucketBuf[bloomOff+16 : bloomOff+24])
			b3 := binary.LittleEndian.Uint64(bucketBuf[bloomOff+24 : bloomOff+32])

			// now generate a mask that is bloomEntIdx bits long
			mLast := uint64(0xffffffffffffffff) >> (63 - (bloomEntIdx % 64))

			var inM1, inM2, inM3 uint64
			bei6 := bloomEntIdx >> 6

			inM2 = (bloomEntIdx >> 7) & 1
			inM1 = inM2 | bei6
			inM3 = inM2 & bei6

			m0 := mLast | (-inM1)
			m1 := (mLast & (-inM1)) | (-inM2)
			m2 := (mLast & (-inM2)) | (-inM3)
			m3 := mLast & (-inM3)

			// count bits
			minOffIdx := (bits.OnesCount64(b0&m0) + bits.OnesCount64(b1&m1) + bits.OnesCount64(b2&m2) + bits.OnesCount64(b3&m3)) - 1
			for entIdx := minOffIdx; entIdx < BucketUserEntries; entIdx++ {
				if bytes.Equal(bucketBuf[entIdx*EntrySize:entIdx*EntrySize+EntKeyBytes], k[:EntKeyBytes]) {
					out[i] = int64(binary.LittleEndian.Uint64(bucketBuf[entIdx*EntrySize+EntKeyBytes : entIdx*EntrySize+EntKeyBytes+8]))
					continue top
				}
			}

			prevLevelBuckets += levelBuckets
			levelBuckets = (levelBuckets + LevelFactor - 1) / LevelFactor
		}
	}

	return out, nil
}

func (h *BSST) Close() error {
	return h.f.Close()
}
