package carlog

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/lotus-web3/ribs/bsst"
	"github.com/minio/sha256-simd"
	mh "github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	LogBufSize = 128 << 10
	LogWriteCh = 128

	LogMaxSize = 2 << 20 // 2 Mi entries, ~64MiB in memory

	StringLogExt = ".sl"
	BsstProgExt  = ".bsst.prog"
	BsstExt      = ".bsst"
)

type LogBsstIndex struct {
	root string // root dir

	// mhh salt
	salt [32]byte

	partsLk    sync.Mutex
	partitions []*partition // last one is write log, rest is compacting or compacted partitions

	writeLk sync.Mutex
}

type partition struct {
	compacting, compacted atomic.Bool // unless compacted read from logIndex

	// Log state

	logFile *os.File

	logIndexLk sync.Mutex // maps ops are fast, so rw lock may actually be slower (todo benchmark)
	logIndex   map[string]int64

	// writing
	writesSent int64
	//writesRecv atomic.Int64

	writeCh chan []byte
	closing bool

	bw *bufio.Writer

	writeFlushed atomic.Int64 // TODO SET ON REOPEN

	// Compacted state
	salt [32]byte

	bss *bsst.BSST
}

func OpenLogBsstIndex(root string) (*LogBsstIndex, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, xerrors.Errorf("mkdirall: %w", err)
	}

	lbi := &LogBsstIndex{
		root: root,
	}

	// Load salt (if not present we're creating a new index)

	// Load partitions
	// * Every non-last log is a compacting log
	// * Every log with .bsst is a compacted log
	//   * If a log with .bsst has a .sl file, it's a finished compaction but .sl wasn't removed (we do that as cleanup on startup)
	// * Logs with .bsst.prog are unfinished compactions
	// * Last log is a write log unless it has a .bsst.prog file, in which case it's a compacting log
	// * If there's no last writable log, new one will be created in Put

	return lbi, nil
}

/*
----------------------------
---------- WRITE ----------
----------------------------
*/

func (l *LogBsstIndex) Put(c []mh.Multihash, offs []int64) error {
	if len(c) != len(offs) {
		return xerrors.New("mismatched input lengths")
	}
	if len(c) == 0 {
		return nil
	}

	// make entries
	/*writeBuf := pool.Get(len(c) * bsst.EntrySize)
	defer pool.Put(writeBuf)*/
	writeBuf := make([]byte, len(c)*bsst.EntrySize)

	for i, h := range c {
		k := l.makeMHKey(h, 0) // always 0th instance, we assume entries don't repeat

		// bsst.EntKeyBytes
		copy(writeBuf[i*bsst.EntrySize:], k[:bsst.EntKeyBytes])
		binary.LittleEndian.PutUint64(writeBuf[i*bsst.EntrySize+bsst.EntKeyBytes:], uint64(offs[i]))
	}

	l.writeLk.Lock()

	{
		// ensure space in current log

		needNewLog := true
		if len(l.partitions) > 0 {
			lastLog := l.partitions[len(l.partitions)-1]
			if lastLog.writesSent+int64(len(c)) < LogMaxSize {
				needNewLog = false
			}
		}

		if needNewLog {
			if err := l.newLog(); err != nil {
				l.writeLk.Unlock()
				return err
			}
		}
	}

	l.partitions[len(l.partitions)-1].writesSent += int64(len(c))
	l.partitions[len(l.partitions)-1].writeCh <- writeBuf

	l.partitions[len(l.partitions)-1].logIndexLk.Lock()
	l.writeLk.Unlock()

	for i, h := range c {
		l.partitions[len(l.partitions)-1].logIndex[string(h)] = offs[i]
	}
	l.partitions[len(l.partitions)-1].logIndexLk.Unlock()

	return nil
}

func (l *LogBsstIndex) newLog() error {
	logPath := filepath.Join(l.root, fmt.Sprintf("log%d.sl", len(l.partitions)))

	f, err := os.Create(logPath)
	if err != nil {
		return xerrors.Errorf("create new log file: %w", err)
	}

	ilo := &partition{
		logFile:  f,
		logIndex: map[string]int64{}, // todo map pool?
		writeCh:  make(chan []byte, LogWriteCh),
		bw:       bufio.NewWriterSize(f, LogBufSize),
		salt:     l.salt,
		// writeFlushed = 0 is correct, new index
	}

	l.partsLk.Lock()

	// close previous last log
	prevLast := l.partitions[len(l.partitions)-1]

	// add new log
	l.partitions = append(l.partitions, ilo)

	l.partsLk.Unlock()

	// if we weren't holding the l.writeLk here this close would have to be under partsLk
	close(prevLast.writeCh)
	prevLast.writeCh = nil

	go ilo.run(0) // new index so no writes yet

	return nil
}

func (l *LogBsstIndex) Del(c []mh.Multihash) error {
	panic("implement me")
}

func (i *partition) run(writesRecv int64) {
	writeCh := i.writeCh

	for b := range writeCh {
		if len(b) == 0 {
			if err := i.flush(writesRecv); err != nil {
				log.Errorf("flushing log: %s", err)
				return
			}
			continue
		}

		writesRecv++

		_, err := i.bw.Write(b)
		if err != nil {
			log.Errorf("writing to log: %s", err)
			return // this will make things hang, but we can't really do anything else (todo wider panik)
		}
	}

	if err := i.flush(writesRecv); err != nil {
		log.Errorf("flushing log: %s", err)
		return
	}

	if i.closing {
		if err := i.logFile.Close(); err != nil {
			log.Errorf("closing log file: %s", err)
			return
		}
		return
	}

	i.compact()
}

func (i *partition) compact() {
	i.compacting.Store(true)

	nonSlPath := strings.TrimSuffix(i.logFile.Name(), StringLogExt)
	bsstPath := nonSlPath + BsstProgExt
	bsstPathFinal := nonSlPath + BsstExt

	// load and sorted log
	data, err := i.mhhSortedLog(i.writeFlushed.Load())
	if err != nil {
		log.Errorf("loading sorted log: %s", err)
		return
	}
	defer pool.Put(data)

	// write bsst
	bss, err := bsst.CreateAdv(bsstPath, int64(data.Len()), i.salt, data.Entry)
	if err != nil {
		log.Errorf("creating bsst: %s", err)
		return // on restart we'll see that the bsst is or isn't correctly created and potentially redo the compaction
	}

	// swap status to compacted
	i.bss = bss // first ready for reads from bsst

	i.compacted.Store(true) // say that reads should be from bsst

	i.logIndexLk.Lock()
	i.logIndex = nil // remove memory index, allow gc
	i.logIndexLk.Unlock()

	// rename bsst to final name
	if err := os.Rename(bsstPath, bsstPathFinal); err != nil {
		log.Errorf("renaming bsst: %s", err)
		return
	}

	if err := i.logFile.Close(); err != nil {
		log.Errorf("closing log file: %s", err)
		return
	}
	i.logFile = nil

	// remove log
	if err := os.Remove(i.logFile.Name()); err != nil {
		log.Errorf("removing log: %s", err)
		return
	}
}

// callers MUST call pool.Put on the returned slice
func (i *partition) mhhSortedLog(sizeEntries int64) (entrySlice, error) {
	logSize := sizeEntries * bsst.EntrySize
	logData := pool.Get(int(logSize))

	_, err := i.logFile.ReadAt(logData, 0)
	if err != nil {
		return nil, err
	}

	data := entrySlice(logData)

	// sort it
	sort.Sort(data)

	return data, nil
}

type entrySlice []byte

func (e entrySlice) Len() int {
	return len(e) / bsst.EntrySize
}

func (e entrySlice) Less(i, j int) bool {
	return bytes.Compare(e[i*bsst.EntrySize:i*bsst.EntrySize+bsst.EntKeyBytes], e[j*bsst.EntrySize:j*bsst.EntKeyBytes+bsst.EntKeyBytes]) < 0
}

func (e entrySlice) Swap(i, j int) {
	ii := i * bsst.EntrySize
	jj := j * bsst.EntrySize

	var temp [bsst.EntrySize]byte

	copy(temp[:], e[ii:ii+bsst.EntrySize])
	copy(e[ii:ii+bsst.EntrySize], e[jj:jj+bsst.EntrySize])
	copy(e[jj:jj+bsst.EntrySize], temp[:])
}

func (e entrySlice) Entry(i int64) []byte {
	return e[i*bsst.EntrySize : (i+1)*bsst.EntrySize]
}

func (i *partition) flush(writesRecv int64) error {
	// note: this method is only called from partition.run

	if writesRecv == 0 {
		return nil
	}

	if err := i.bw.Flush(); err != nil {
		return xerrors.Errorf("flushing log: %w", err)
	}

	if err := i.logFile.Sync(); err != nil {
		return xerrors.Errorf("syncing log: %w", err)
	}

	i.writeFlushed.Store(writesRecv)
	return nil
}

// MUST be called with LogBsstIndex.writeLk held
func (i *partition) startSync() int64 {
	if i.writeCh == nil {
		// either closing on compaction requested. in both cases that stops writes
		// and triggers a flush, so we can just return the current writesSent
		return i.writesSent
	}

	waitUntil := i.writesSent
	i.writeCh <- nil
	return waitUntil
}

const (
	syncPollIntervalMax = 20 * time.Millisecond
	syncPollIntervalMin = 1 * time.Microsecond

	syncTimeout = 2 * time.Minute
)

func (i *partition) waitSync(waitUntil int64) error {
	nextWait := syncPollIntervalMin
	start := time.Now()

	for i.writeFlushed.Load() < waitUntil {
		time.Sleep(nextWait)

		if time.Since(start) > syncTimeout {
			return xerrors.Errorf("sync timeout")
		}

		if nextWait < syncPollIntervalMax {
			nextWait *= 2
		} else {
			nextWait = syncPollIntervalMax
		}
	}

	return nil
}

/*
----------------------------
----------- READ -----------
----------------------------
*/

func (l *LogBsstIndex) Has(c []mh.Multihash) ([]bool, error) {
	l.partsLk.Lock()
	partitions := l.partitions  // copy so that appends in write don't swap the slice under us
	partsLen := len(partitions) // append can change len too, so save it here
	l.partsLk.Unlock()

	results := make([]bool, len(c))

	for i, multihash := range c {
		// look through partitions starting with last (reads into recent writes are more likely)
		for p := partsLen - 1; p >= 0; p-- {
			part := partitions[p]
			if !part.compacted.Load() {
				// it's maybe not a compacted partition
				part.logIndexLk.Lock()

				// compaction may finish before taking logIndexLk, so check again
				if part.logIndex != nil {
					if _, ok := part.logIndex[string(multihash)]; ok {
						results[i] = true
						part.logIndexLk.Unlock()
						break
					}
				}

				part.logIndexLk.Unlock()
			}

			// we HAVE TO load again, compaction may have juust finished
			if part.compacted.Load() {
				off, err := part.bss.Has([]mh.Multihash{multihash}) // todo HasSingle, not much advantage here from slices
				if err != nil {
					return nil, err
				}

				if off[0] {
					results[i] = true
					break
				}
			}
		}
	}

	return results, nil
}

func (l *LogBsstIndex) Get(c []mh.Multihash) ([]int64, error) {
	l.partsLk.Lock()
	partitions := l.partitions  // copy so that appends in write don't swap the slice under us
	partsLen := len(partitions) // append can change len too, so save it here
	l.partsLk.Unlock()

	results := make([]int64, len(c))

	for i, multihash := range c {
		results[i] = -1

		// look through partitions starting with last (reads into recent writes are more likely)
		for p := partsLen - 1; p >= 0; p-- {
			part := partitions[p]
			if !part.compacted.Load() {
				// it's maybe not a compacted partition
				part.logIndexLk.Lock()

				// compaction may finish before taking logIndexLk, so check again
				if part.logIndex != nil {
					if off, ok := part.logIndex[string(multihash)]; ok {
						results[i] = off
						part.logIndexLk.Unlock()
						break
					}
				}

				part.logIndexLk.Unlock()
			}

			// we HAVE TO load again, compaction may have juust finished
			if part.compacted.Load() {
				off, err := part.bss.Get([]mh.Multihash{multihash}) // todo GetSingle, not much advantage here from slices
				if err != nil {
					return nil, err
				}

				if off[0] != -1 {
					results[i] = off[0]
					break
				}
			}

		}
	}

	return results, nil
}

func (l *LogBsstIndex) Entries() (int64, error) {
	l.partsLk.Lock()
	defer l.partsLk.Unlock()

	var total int64
	for _, p := range l.partitions {
		total += p.writeFlushed.Load()
	}

	return total, nil
}

func (l *LogBsstIndex) List(f func(c mh.Multihash, offs []int64) error) error {
	return xerrors.Errorf("use advanced")
}

func (l *LogBsstIndex) ListAdv(f func([32]byte) error) error {
	// don't allow writes while outputting the list
	// writes shouldn't be routed into this index anyways when this is called

	l.writeLk.Lock()
	defer l.writeLk.Unlock()

	l.partsLk.Lock()            // technically we don't even need this lock, but might as well take it
	partitions := l.partitions  // copy so that appends in write don't swap the slice under us
	partsLen := len(partitions) // append can change len too, so save it here
	l.partsLk.Unlock()

	syncs := make([]int64, partsLen)

	for p := 0; p < partsLen; p++ {
		syncs[p] = partitions[p].startSync()
	}
	for p := 0; p < partsLen; p++ {
		if err := partitions[p].waitSync(syncs[p]); err != nil {
			return xerrors.Errorf("waiting for sync: %w", err)
		}
	}

	partitionIterators := make([]func() ([32]byte, error), partsLen)

	for p := 0; p < partsLen; p++ {
		part := partitions[p]

		if !part.compacted.Load() {
			// not compacted, soo:
			// * if in process of compaction, wait for it to finish
			// * if not in process of compaction, read and sort the log

			if !part.compacting.Load() {
				// not compacting, read from log
				// sync is done, so we just read the log file and sort it like in compaction

				part.logIndexLk.Lock()

				// one last, last level of making sure we're not reading from a compacted partition
				// if part.logIndex is nil, the partition somehow got compacted between now and reading part.compacting above
				if part.logIndex != nil {
					data, err := part.mhhSortedLog(syncs[p])
					part.logIndexLk.Unlock()

					if err != nil {
						return xerrors.Errorf("loading sorted log for partition %d: %w", p, err)
					}

					partitionIterators[p] = func() ([32]byte, error) {

					}

					continuee
				}
				part.logIndexLk.Unlock()

				// if we're here, magically somehow the partition got compacted. This is weird but actually fine
				partitionIterators[p] = func() ([32]byte, error) {

				}

			}

			// compaction in progress, wait for it to finish
			for !part.compacted.Load() {
				time.Sleep(50 * time.Millisecond)
			}
		}

		// definitely compacted here, read from bss

	}

}

func (l *LogBsstIndex) ToTruncate(atOrAbove int64) ([]mh.Multihash, error) {
	//TODO implement me
	panic("implement me")
}

func (l *LogBsstIndex) Close() error {
	//TODO implement me
	panic("implement me")
}

type multiHashHash struct {
	mhh [32]byte
	off int64
}

func (l *LogBsstIndex) makeMHH(c mh.Multihash, i int64, off int64) multiHashHash {
	return multiHashHash{
		mhh: l.makeMHKey(c, i),
		off: off,
	}
}

func (l *LogBsstIndex) makeMHKey(c mh.Multihash, i int64) [32]byte {
	// buf = [salt][i: le64][c[:64]]
	var buf [(32 + 8) + (32 * 2)]byte
	copy(buf[:], l.salt[:])
	binary.LittleEndian.PutUint64(buf[32:], uint64(i))
	copy(buf[32+8:], c)

	return sha256.Sum256(buf[:])
}

var _ WritableIndex = (*LogBsstIndex)(nil)
var _ ReadableIndex = (*LogBsstIndex)(nil)
