// Package valuestore provides a disk-backed data structure for use in storing
// []byte values referenced by 128 bit keys with options for replication.
//
// It can handle billions of keys (as memory allows) and full concurrent access
// across many cores. All location information about each key is stored in
// memory for speed, but values are stored on disk with the exception of
// recently written data being buffered first and batched to disk later.
//
// This has been written with SSDs in mind, but spinning drives should work as
// well; though storing valuestoc files (Table Of Contents, key location
// information) on a separate disk from values files is recommended in that
// case.
//
// Each key is two 64bit values, known as keyA and keyB uint64 values. These
// are usually created by a hashing function of the key name, but that duty is
// left outside this package.
//
// Each modification is recorded with an int64 timestamp that is number of
// microseconds since the Unix epoch (see
// github.com/gholt/brimtime.TimeToUnixMicro). With a write and delete for the
// exact same timestamp, the delete wins. This allows a delete to be issued for
// a specific write without fear of deleting any newer write.
//
// Internally, each modification is stored with a uint64 timestamp that is
// equivalent to (brimtime.TimeToUnixMicro(time.Now())<<8) with the lowest 8
// bits used to indicate deletions and other bookkeeping items. This means that
// the allowable time range is 1970-01-01 00:00:00 +0000 UTC (+1 microsecond
// because all zeroes indicates a missing item) to 4253-05-31 22:20:37.927935
// +0000 UTC. There are constants TIMESTAMPMICRO_MIN and TIMESTAMPMICRO_MAX
// available for bounding usage.
//
// There are background tasks for:
//
// * TombstoneDiscard: This will discard older tombstones (deletion markers).
// Tombstones are kept for OptTombstoneAge seconds and are used to ensure a
// replicated older value doesn't resurrect a deleted value. But, keeping all
// tombstones for all time is a waste of resources, so they are discarded over
// time. OptTombstoneAge controls how long they should be kept and should be
// set to an amount greater than several replication passes.
//
// * PullReplication: This will continually send out pull replication requests
// for all the partitions the ValueStore is responsible for, as determined by
// the OptMsgRing. The other responsible parties will respond to these requests
// with data they have that was missing from the pull replication request.
// Bloom filters are used to reduce bandwidth which has the downside that a
// very small percentage of items may be missed each pass. A moving salt is
// used with each bloom filter so that after a few passes there is an
// exceptionally high probability that all items will be accounted for.
//
// * PushReplication: This will continually send out any data for any
// partitions the ValueStore is *not* responsible for, as determined by the
// OptMsgRing. The responsible parties will respond to these requests with
// acknowledgements of the data they received, allowing the requester to
// discard the out of place data.
package valuestore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gholt/ring"
	"github.com/gholt/valuelocmap"
	"github.com/spaolacci/murmur3"
	"gopkg.in/gholt/brimutil.v1"
)

const (
	_TSB_UTIL_BITS = 8
	_TSB_INACTIVE  = 0xff
	_TSB_DELETION  = 0x80
	// _TSB_COMPACTION_REWRITE indicates an item is being rewritten as part of
	// compaction to the last disk file.
	_TSB_COMPACTION_REWRITE = 0x01
	// _TSB_LOCAL_REMOVAL indicates an item to be removed locally due to push
	// replication (local store wasn't considered responsible for the item
	// according to the ring) or a deletion marker expiration. An item marked
	// for local removal will be retained in memory until the local removal
	// marker is written to disk.
	_TSB_LOCAL_REMOVAL = 0x02
)

const (
	TIMESTAMPMICRO_MIN = int64(uint64(1) << _TSB_UTIL_BITS)
	TIMESTAMPMICRO_MAX = int64(uint64(math.MaxUint64) >> _TSB_UTIL_BITS)
)

const _GLH_RECOVERY_BATCH_SIZE = 1024 * 1024

// ValueStore is an interface for a disk-backed data structure that stores
// []byte values referenced by 128 bit keys with options for replication.
//
// For documentation on each of these functions, see the DefaultValueStore.
type ValueStore interface {
	Lookup(keyA uint64, keyB uint64) (int64, uint32, error)
	Read(keyA uint64, keyB uint64, value []byte) (int64, []byte, error)
	Write(keyA uint64, keyB uint64, timestamp int64, value []byte) (int64, error)
	Delete(keyA uint64, keyB uint64, timestamp int64) (int64, error)
	EnableAll()
	DisableAll()
	DisableAllBackground()
	EnableTombstoneDiscard()
	DisableTombstoneDiscard()
	TombstoneDiscardPass()
	EnableCompaction()
	DisableCompaction()
	CompactionPass()
	EnableOutPullReplication()
	DisableOutPullReplication()
	OutPullReplicationPass()
	EnableOutPushReplication()
	DisableOutPushReplication()
	OutPushReplicationPass()
	EnableWrites()
	DisableWrites()
	Flush()
	GatherStats(debug bool) (uint64, uint64, fmt.Stringer)
	MaxValueSize() uint32
}

var ErrNotFound error = errors.New("not found")
var ErrDisabled error = errors.New("disabled")

// DefaultValueStore instances are created with New.
type DefaultValueStore struct {
	logCritical             *log.Logger
	logError                *log.Logger
	logWarning              *log.Logger
	logInfo                 *log.Logger
	logDebug                *log.Logger
	rand                    *rand.Rand
	freeableVMChans         []chan *valuesMem
	freeVMChan              chan *valuesMem
	freeVWRChans            []chan *valueWriteReq
	pendingVWRChans         []chan *valueWriteReq
	vfVMChan                chan *valuesMem
	freeTOCBlockChan        chan []byte
	pendingTOCBlockChan     chan []byte
	activeTOCA              uint64
	activeTOCB              uint64
	flushedChan             chan struct{}
	valueLocBlocks          []valueLocBlock
	valueLocBlockIDer       uint64
	path                    string
	pathtoc                 string
	vlm                     valuelocmap.ValueLocMap
	workers                 int
	maxValueSize            uint32
	pageSize                uint32
	minValueAlloc           int
	writePagesPerWorker     int
	valuesFileSize          uint32
	valuesFileReaders       int
	checksumInterval        uint32
	msgRing                 ring.MsgRing
	tombstoneDiscardState   tombstoneDiscardState
	replicationIgnoreRecent uint64
	pullReplicationState    pullReplicationState
	pushReplicationState    pushReplicationState
	compactionState         compactionState
	bulkSetState            bulkSetState
	bulkSetAckState         bulkSetAckState
}

type valueWriteReq struct {
	keyA          uint64
	keyB          uint64
	timestampbits uint64
	value         []byte
	errChan       chan error
}

var enableValueWriteReq *valueWriteReq = &valueWriteReq{}
var disableValueWriteReq *valueWriteReq = &valueWriteReq{}
var flushValueWriteReq *valueWriteReq = &valueWriteReq{}
var flushValuesMem *valuesMem = &valuesMem{}

type valueLocBlock interface {
	timestampnano() int64
	read(keyA uint64, keyB uint64, timestampbits uint64, offset uint32, length uint32, value []byte) (uint64, []byte, error)
}

type backgroundNotification struct {
	enable   bool
	disable  bool
	doneChan chan struct{}
}

// New creates a DefaultValueStore for use in storing []byte values referenced
// by 128 bit keys.
//
// Note that a lot of buffering, multiple cores, and background processes can
// be in use and therefore DisableAll() and Flush() should be called prior to
// the process exiting to ensure all processing is done and the buffers are
// flushed.
//
// You can provide Opt* functions for optional configuration items, such as
// OptWorkers:
//
//  vsWithDefaults := valuestore.New()
//  vsWithOptions := valuestore.New(
//      valuestore.OptWorkers(10),
//      valuestore.OptPageSize(8388608),
//  )
//  opts := valuestore.OptList()
//  if commandLineOptionForWorkers {
//      opts = append(opts, valuestore.OptWorkers(commandLineOptionValue))
//  }
//  vsWithOptionsBuiltUp := valuestore.New(opts...)
func New(opts ...func(*config)) *DefaultValueStore {
	cfg := resolveConfig(opts...)
	vlm := cfg.vlm
	if vlm == nil {
		vlm = valuelocmap.New()
	}
	vs := &DefaultValueStore{
		logCritical:             cfg.logCritical,
		logError:                cfg.logError,
		logWarning:              cfg.logWarning,
		logInfo:                 cfg.logInfo,
		logDebug:                cfg.logDebug,
		rand:                    cfg.rand,
		valueLocBlocks:          make([]valueLocBlock, math.MaxUint16),
		path:                    cfg.path,
		pathtoc:                 cfg.pathtoc,
		vlm:                     vlm,
		workers:                 cfg.workers,
		replicationIgnoreRecent: (uint64(cfg.replicationIgnoreRecent) * uint64(time.Second) / 1000) << _TSB_UTIL_BITS,
		maxValueSize:            uint32(cfg.maxValueSize),
		pageSize:                uint32(cfg.pageSize),
		minValueAlloc:           cfg.minValueAlloc,
		writePagesPerWorker:     cfg.writePagesPerWorker,
		valuesFileSize:          uint32(cfg.valuesFileSize),
		valuesFileReaders:       cfg.valuesFileReaders,
		checksumInterval:        uint32(cfg.checksumInterval),
		msgRing:                 cfg.msgRing,
	}
	vs.freeableVMChans = make([]chan *valuesMem, vs.workers)
	for i := 0; i < cap(vs.freeableVMChans); i++ {
		vs.freeableVMChans[i] = make(chan *valuesMem, vs.workers)
	}
	vs.freeVMChan = make(chan *valuesMem, vs.workers*vs.writePagesPerWorker)
	vs.freeVWRChans = make([]chan *valueWriteReq, vs.workers)
	vs.pendingVWRChans = make([]chan *valueWriteReq, vs.workers)
	vs.vfVMChan = make(chan *valuesMem, vs.workers)
	vs.freeTOCBlockChan = make(chan []byte, vs.workers*2)
	vs.pendingTOCBlockChan = make(chan []byte, vs.workers)
	vs.flushedChan = make(chan struct{}, 1)
	for i := 0; i < cap(vs.freeVMChan); i++ {
		vm := &valuesMem{
			vs:     vs,
			toc:    make([]byte, 0, vs.pageSize),
			values: make([]byte, 0, vs.pageSize),
		}
		vm.id = vs.addValueLocBlock(vm)
		vs.freeVMChan <- vm
	}
	for i := 0; i < len(vs.freeVWRChans); i++ {
		vs.freeVWRChans[i] = make(chan *valueWriteReq, vs.workers*2)
		for j := 0; j < vs.workers*2; j++ {
			vs.freeVWRChans[i] <- &valueWriteReq{errChan: make(chan error, 1)}
		}
	}
	for i := 0; i < len(vs.pendingVWRChans); i++ {
		vs.pendingVWRChans[i] = make(chan *valueWriteReq)
	}
	for i := 0; i < cap(vs.freeTOCBlockChan); i++ {
		vs.freeTOCBlockChan <- make([]byte, 0, vs.pageSize)
	}
	go vs.tocWriter()
	go vs.vfWriter()
	for i := 0; i < len(vs.freeableVMChans); i++ {
		go vs.memClearer(vs.freeableVMChans[i])
	}
	for i := 0; i < len(vs.pendingVWRChans); i++ {
		go vs.memWriter(vs.pendingVWRChans[i])
	}
	vs.recovery()
	vs.tombstoneDiscardInit(cfg)
	vs.compactionInit(cfg)
	vs.pullReplicationInit(cfg)
	vs.pushReplicationInit(cfg)
	vs.bulkSetInit(cfg)
	vs.bulkSetAckInit(cfg)
	return vs
}

// MaxValueSize returns the maximum length of a value the ValueStore can
// accept.
func (vs *DefaultValueStore) MaxValueSize() uint32 {
	return vs.maxValueSize
}

// DisableAll calls DisableAllBackground(), and DisableWrites().
func (vs *DefaultValueStore) DisableAll() {
	vs.DisableAllBackground()
	vs.DisableWrites()
}

// DisableAllBackground calls DisableTombstoneDiscard(), DisableCompaction(),
// DisableOutPullReplication(), DisableOutPushReplication(), but does *not*
// call DisableWrites().
func (vs *DefaultValueStore) DisableAllBackground() {
	vs.DisableTombstoneDiscard()
	vs.DisableCompaction()
	vs.DisableOutPullReplication()
	vs.DisableOutPushReplication()
}

// EnableAll calls EnableTombstoneDiscard(), EnableCompaction(),
// EnableOutPullReplication(), EnableOutPushReplication(), and EnableWrites().
func (vs *DefaultValueStore) EnableAll() {
	vs.EnableTombstoneDiscard()
	vs.EnableOutPullReplication()
	vs.EnableOutPushReplication()
	vs.EnableWrites()
	vs.EnableCompaction()
}

// DisableWrites will cause any incoming Write or Delete requests to respond
// with ErrDisabled until EnableWrites is called.
func (vs *DefaultValueStore) DisableWrites() {
	for _, c := range vs.pendingVWRChans {
		c <- disableValueWriteReq
	}
}

// EnableWrites will resume accepting incoming Write and Delete requests.
func (vs *DefaultValueStore) EnableWrites() {
	for _, c := range vs.pendingVWRChans {
		c <- enableValueWriteReq
	}
}

// Flush will ensure buffered data (at the time of the call) is written to
// disk.
func (vs *DefaultValueStore) Flush() {
	for _, c := range vs.pendingVWRChans {
		c <- flushValueWriteReq
	}
	<-vs.flushedChan
}

// Lookup will return timestampmicro, length, err for keyA, keyB.
//
// Note that err == ErrNotFound with timestampmicro == 0 indicates keyA, keyB
// was not known at all whereas err == ErrNotFound with timestampmicro != 0
// indicates keyA, keyB was known and had a deletion marker (aka tombstone).
func (vs *DefaultValueStore) Lookup(keyA uint64, keyB uint64) (int64, uint32, error) {
	timestampbits, _, length, err := vs.lookup(keyA, keyB)
	return int64(timestampbits >> _TSB_UTIL_BITS), length, err
}

func (vs *DefaultValueStore) lookup(keyA, keyB uint64) (uint64, uint32, uint32, error) {
	timestampbits, id, _, length := vs.vlm.Get(keyA, keyB)
	if id == 0 || timestampbits&_TSB_DELETION != 0 {
		return timestampbits, id, 0, ErrNotFound
	}
	return timestampbits, id, length, nil
}

// Read will return timestampmicro, value, err for keyA, keyB; if an incoming
// value is provided, the read value will be appended to it and the whole
// returned (useful to reuse an existing []byte).
//
// Note that err == ErrNotFound with timestampmicro == 0 indicates keyA, keyB
// was not known at all whereas err == ErrNotFound with timestampmicro != 0
// indicates keyA, keyB was known and had a deletion marker (aka tombstone).
func (vs *DefaultValueStore) Read(keyA uint64, keyB uint64, value []byte) (int64, []byte, error) {
	timestampbits, value, err := vs.read(keyA, keyB, value)
	return int64(timestampbits >> _TSB_UTIL_BITS), value, err
}

func (vs *DefaultValueStore) read(keyA uint64, keyB uint64, value []byte) (uint64, []byte, error) {
	timestampbits, id, offset, length := vs.vlm.Get(keyA, keyB)
	if id == 0 || timestampbits&_TSB_DELETION != 0 {
		return timestampbits, value, ErrNotFound
	}
	return vs.valueLocBlock(id).read(keyA, keyB, timestampbits, offset, length, value)
}

// Write stores timestampmicro, value for keyA, keyB and returns the previously
// stored timestampmicro or returns any error; a newer timestampmicro already
// in place is not reported as an error. Note that with a write and a delete
// for the exact same timestampmicro, the delete wins.
func (vs *DefaultValueStore) Write(keyA uint64, keyB uint64, timestampmicro int64, value []byte) (int64, error) {
	if timestampmicro < TIMESTAMPMICRO_MIN {
		return 0, fmt.Errorf("timestamp %d < %d", timestampmicro, TIMESTAMPMICRO_MIN)
	}
	if timestampmicro > TIMESTAMPMICRO_MAX {
		return 0, fmt.Errorf("timestamp %d > %d", timestampmicro, TIMESTAMPMICRO_MAX)
	}
	timestampbits, err := vs.write(keyA, keyB, uint64(timestampmicro)<<_TSB_UTIL_BITS, value)
	return int64(timestampbits >> _TSB_UTIL_BITS), err
}

func (vs *DefaultValueStore) write(keyA uint64, keyB uint64, timestampbits uint64, value []byte) (uint64, error) {
	i := int(keyA>>1) % len(vs.freeVWRChans)
	vwr := <-vs.freeVWRChans[i]
	vwr.keyA = keyA
	vwr.keyB = keyB
	vwr.timestampbits = timestampbits
	vwr.value = value
	vs.pendingVWRChans[i] <- vwr
	err := <-vwr.errChan
	ptimestampbits := vwr.timestampbits
	vwr.value = nil
	vs.freeVWRChans[i] <- vwr
	return ptimestampbits, err
}

// Delete stores timestampmicro for keyA, keyB and returns the previously
// stored timestampmicro or returns any error; a newer timestampmicro already
// in place is not reported as an error. Note that with a write and a delete
// for the exact same timestampmicro, the delete wins.
func (vs *DefaultValueStore) Delete(keyA uint64, keyB uint64, timestampmicro int64) (int64, error) {
	if timestampmicro < TIMESTAMPMICRO_MIN {
		return 0, fmt.Errorf("timestamp %d < %d", timestampmicro, TIMESTAMPMICRO_MIN)
	}
	if timestampmicro > TIMESTAMPMICRO_MAX {
		return 0, fmt.Errorf("timestamp %d > %d", timestampmicro, TIMESTAMPMICRO_MAX)
	}
	ptimestampbits, err := vs.write(keyA, keyB, (uint64(timestampmicro)<<_TSB_UTIL_BITS)|_TSB_DELETION, nil)
	return int64(ptimestampbits >> _TSB_UTIL_BITS), err
}

func (vs *DefaultValueStore) valueLocBlock(valueLocBlockID uint32) valueLocBlock {
	return vs.valueLocBlocks[valueLocBlockID]
}

func (vs *DefaultValueStore) addValueLocBlock(block valueLocBlock) uint32 {
	id := atomic.AddUint64(&vs.valueLocBlockIDer, 1)
	if id >= math.MaxUint32 {
		panic("too many valueLocBlocks")
	}
	vs.valueLocBlocks[id] = block
	return uint32(id)
}

func (vs *DefaultValueStore) valueLocBlockIDFromTimestampnano(tsn int64) uint32 {
	for i := 1; i <= len(vs.valueLocBlocks); i++ {
		if vs.valueLocBlocks[i] == nil {
			return 0
		} else {
			if tsn == vs.valueLocBlocks[i].timestampnano() {
				return uint32(i)
			}
		}
	}
	return 0
}

func (vs *DefaultValueStore) memClearer(freeableVMChan chan *valuesMem) {
	var tb []byte
	var tbTS int64
	var tbOffset int
	for {
		vm := <-freeableVMChan
		if vm == flushValuesMem {
			if tb != nil {
				vs.pendingTOCBlockChan <- tb
				tb = nil
			}
			vs.pendingTOCBlockChan <- nil
			continue
		}
		vf := vs.valueLocBlock(vm.vfID)
		if tb != nil && tbTS != vf.timestampnano() {
			vs.pendingTOCBlockChan <- tb
			tb = nil
		}
		for vmTOCOffset := 0; vmTOCOffset < len(vm.toc); vmTOCOffset += 32 {
			keyA := binary.BigEndian.Uint64(vm.toc[vmTOCOffset:])
			keyB := binary.BigEndian.Uint64(vm.toc[vmTOCOffset+8:])
			timestampbits := binary.BigEndian.Uint64(vm.toc[vmTOCOffset+16:])
			var blockID uint32
			var offset uint32
			var length uint32
			if timestampbits&_TSB_LOCAL_REMOVAL == 0 {
				blockID = vm.vfID
				offset = vm.vfOffset + binary.BigEndian.Uint32(vm.toc[vmTOCOffset+24:])
				length = binary.BigEndian.Uint32(vm.toc[vmTOCOffset+28:])
			}
			if vs.vlm.Set(keyA, keyB, timestampbits, blockID, offset, length, true) > timestampbits {
				continue
			}
			if tb != nil && tbOffset+32 > cap(tb) {
				vs.pendingTOCBlockChan <- tb
				tb = nil
			}
			if tb == nil {
				tb = <-vs.freeTOCBlockChan
				tbTS = vf.timestampnano()
				tb = tb[:8]
				binary.BigEndian.PutUint64(tb, uint64(tbTS))
				tbOffset = 8
			}
			tb = tb[:tbOffset+32]
			binary.BigEndian.PutUint64(tb[tbOffset:], keyA)
			binary.BigEndian.PutUint64(tb[tbOffset+8:], keyB)
			binary.BigEndian.PutUint64(tb[tbOffset+16:], timestampbits)
			binary.BigEndian.PutUint32(tb[tbOffset+24:], offset)
			binary.BigEndian.PutUint32(tb[tbOffset+28:], length)
			tbOffset += 32
		}
		vm.discardLock.Lock()
		vm.vfID = 0
		vm.vfOffset = 0
		vm.toc = vm.toc[:0]
		vm.values = vm.values[:0]
		vm.discardLock.Unlock()
		vs.freeVMChan <- vm
	}
}

func (vs *DefaultValueStore) memWriter(pendingVWRChan chan *valueWriteReq) {
	var enabled bool
	var vm *valuesMem
	var vmTOCOffset int
	var vmMemOffset int
	for {
		vwr := <-pendingVWRChan
		if vwr == enableValueWriteReq {
			enabled = true
			continue
		}
		if vwr == disableValueWriteReq {
			enabled = false
			continue
		}
		if vwr == flushValueWriteReq {
			if vm != nil && len(vm.toc) > 0 {
				vs.vfVMChan <- vm
				vm = nil
			}
			vs.vfVMChan <- flushValuesMem
			continue
		}
		if !enabled {
			vwr.errChan <- ErrDisabled
			continue
		}
		length := len(vwr.value)
		if length > int(vs.maxValueSize) {
			vwr.errChan <- fmt.Errorf("value length of %d > %d", length, vs.maxValueSize)
			continue
		}
		alloc := length
		if alloc < vs.minValueAlloc {
			alloc = vs.minValueAlloc
		}
		if vm != nil && (vmTOCOffset+32 > cap(vm.toc) || vmMemOffset+alloc > cap(vm.values)) {
			vs.vfVMChan <- vm
			vm = nil
		}
		if vm == nil {
			vm = <-vs.freeVMChan
			vmTOCOffset = 0
			vmMemOffset = 0
		}
		vm.discardLock.Lock()
		vm.values = vm.values[:vmMemOffset+alloc]
		vm.discardLock.Unlock()
		copy(vm.values[vmMemOffset:], vwr.value)
		if alloc > length {
			for i, j := vmMemOffset+length, vmMemOffset+alloc; i < j; i++ {
				vm.values[i] = 0
			}
		}
		ptimestampbits := vs.vlm.Set(vwr.keyA, vwr.keyB, vwr.timestampbits, vm.id, uint32(vmMemOffset), uint32(length), false)
		if ptimestampbits < vwr.timestampbits {
			vm.toc = vm.toc[:vmTOCOffset+32]
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset:], vwr.keyA)
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset+8:], vwr.keyB)
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset+16:], vwr.timestampbits)
			binary.BigEndian.PutUint32(vm.toc[vmTOCOffset+24:], uint32(vmMemOffset))
			binary.BigEndian.PutUint32(vm.toc[vmTOCOffset+28:], uint32(length))
			vmTOCOffset += 32
			vmMemOffset += alloc
		} else {
			vm.discardLock.Lock()
			vm.values = vm.values[:vmMemOffset]
			vm.discardLock.Unlock()
		}
		vwr.timestampbits = ptimestampbits
		vwr.errChan <- nil
	}
}

func (vs *DefaultValueStore) vfWriter() {
	var vf *valuesFile
	memWritersFlushLeft := len(vs.pendingVWRChans)
	var tocLen uint64
	var valueLen uint64
	for {
		vm := <-vs.vfVMChan
		if vm == flushValuesMem {
			memWritersFlushLeft--
			if memWritersFlushLeft > 0 {
				continue
			}
			if vf != nil {
				vf.close()
				vf = nil
			}
			for i := 0; i < len(vs.freeableVMChans); i++ {
				vs.freeableVMChans[i] <- flushValuesMem
			}
			memWritersFlushLeft = len(vs.pendingVWRChans)
			continue
		}
		if vf != nil && (tocLen+uint64(len(vm.toc)) >= uint64(vs.valuesFileSize) || valueLen+uint64(len(vm.values)) > uint64(vs.valuesFileSize)) {
			vf.close()
			vf = nil
		}
		if vf == nil {
			vf = createValuesFile(vs)
			tocLen = 32
			valueLen = 32
		}
		vf.write(vm)
		tocLen += uint64(len(vm.toc))
		valueLen += uint64(len(vm.values))
	}
}

func (vs *DefaultValueStore) tocWriter() {
	// writerA is the current toc file while writerB is the previously active toc
	// writerB is kept around in case a "late" key arrives to be flushed whom's value
	// is actually in the previous values file.
	memClearersFlushLeft := len(vs.freeableVMChans)
	var writerA io.WriteCloser
	var offsetA uint64
	var writerB io.WriteCloser
	var offsetB uint64
	head := []byte("VALUESTORETOC v0                ")
	binary.BigEndian.PutUint32(head[28:], uint32(vs.checksumInterval))
	term := make([]byte, 16)
	copy(term[12:], "TERM")
	for {
		t := <-vs.pendingTOCBlockChan
		if t == nil {
			memClearersFlushLeft--
			if memClearersFlushLeft > 0 {
				continue
			}
			if writerB != nil {
				binary.BigEndian.PutUint64(term[4:], offsetB)
				if _, err := writerB.Write(term); err != nil {
					panic(err)
				}
				if err := writerB.Close(); err != nil {
					panic(err)
				}
				writerB = nil
				atomic.StoreUint64(&vs.activeTOCB, 0)
				offsetB = 0
			}
			if writerA != nil {
				binary.BigEndian.PutUint64(term[4:], offsetA)
				if _, err := writerA.Write(term); err != nil {
					panic(err)
				}
				if err := writerA.Close(); err != nil {
					panic(err)
				}
				writerA = nil
				atomic.StoreUint64(&vs.activeTOCA, 0)
				offsetA = 0
			}
			vs.flushedChan <- struct{}{}
			memClearersFlushLeft = len(vs.freeableVMChans)
			continue
		}
		if len(t) > 8 {
			bts := binary.BigEndian.Uint64(t)
			switch bts {
			case atomic.LoadUint64(&vs.activeTOCA):
				if _, err := writerA.Write(t[8:]); err != nil {
					panic(err)
				}
				offsetA += uint64(len(t) - 8)
			case atomic.LoadUint64(&vs.activeTOCB):
				if _, err := writerB.Write(t[8:]); err != nil {
					panic(err)
				}
				offsetB += uint64(len(t) - 8)
			default:
				// An assumption is made here: If the timestampnano for this
				// toc block doesn't match the last two seen timestampnanos
				// then we expect no more toc blocks for the oldest
				// timestampnano and can close that toc file.
				if writerB != nil {
					binary.BigEndian.PutUint64(term[4:], offsetB)
					if _, err := writerB.Write(term); err != nil {
						panic(err)
					}
					if err := writerB.Close(); err != nil {
						panic(err)
					}
				}
				atomic.StoreUint64(&vs.activeTOCB, atomic.LoadUint64(&vs.activeTOCA))
				writerB = writerA
				offsetB = offsetA
				atomic.StoreUint64(&vs.activeTOCA, bts)
				fp, err := os.Create(path.Join(vs.pathtoc, fmt.Sprintf("%d.valuestoc", bts)))
				if err != nil {
					panic(err)
				}
				writerA = brimutil.NewMultiCoreChecksummedWriter(fp, int(vs.checksumInterval), murmur3.New32, vs.workers)
				if _, err := writerA.Write(head); err != nil {
					panic(err)
				}
				if _, err := writerA.Write(t[8:]); err != nil {
					panic(err)
				}
				offsetA = 32 + uint64(len(t)-8)
			}
		}
		vs.freeTOCBlockChan <- t[:0]
	}
}

func (vs *DefaultValueStore) recovery() {
	start := time.Now()
	fromDiskCount := 0
	causedChangeCount := int64(0)
	type writeReq struct {
		keyA          uint64
		keyB          uint64
		timestampbits uint64
		blockID       uint32
		offset        uint32
		length        uint32
	}
	workers := uint64(vs.workers)
	pendingBatchChans := make([]chan []writeReq, workers)
	freeBatchChans := make([]chan []writeReq, len(pendingBatchChans))
	for i := 0; i < len(pendingBatchChans); i++ {
		pendingBatchChans[i] = make(chan []writeReq, 4)
		freeBatchChans[i] = make(chan []writeReq, 4)
		for j := 0; j < cap(freeBatchChans[i]); j++ {
			freeBatchChans[i] <- make([]writeReq, _GLH_RECOVERY_BATCH_SIZE)
		}
	}
	wg := &sync.WaitGroup{}
	wg.Add(len(pendingBatchChans))
	for i := 0; i < len(pendingBatchChans); i++ {
		go func(pendingBatchChan chan []writeReq, freeBatchChan chan []writeReq) {
			for {
				batch := <-pendingBatchChan
				if batch == nil {
					break
				}
				for j := 0; j < len(batch); j++ {
					wr := &batch[j]
					if wr.timestampbits&_TSB_LOCAL_REMOVAL != 0 {
						wr.blockID = 0
					}
					if vs.logDebug != nil {
						if vs.vlm.Set(wr.keyA, wr.keyB, wr.timestampbits, wr.blockID, wr.offset, wr.length, true) < wr.timestampbits {
							atomic.AddInt64(&causedChangeCount, 1)
						}
					} else {
						vs.vlm.Set(wr.keyA, wr.keyB, wr.timestampbits, wr.blockID, wr.offset, wr.length, true)
					}
				}
				freeBatchChan <- batch
			}
			wg.Done()
		}(pendingBatchChans[i], freeBatchChans[i])
	}
	fromDiskBuf := make([]byte, vs.checksumInterval+4)
	fromDiskOverflow := make([]byte, 0, 32)
	batches := make([][]writeReq, len(freeBatchChans))
	batchesPos := make([]int, len(batches))
	fp, err := os.Open(vs.pathtoc)
	if err != nil {
		panic(err)
	}
	names, err := fp.Readdirnames(-1)
	fp.Close()
	if err != nil {
		panic(err)
	}
	sort.Strings(names)
	for i := 0; i < len(names); i++ {
		if !strings.HasSuffix(names[i], ".valuestoc") {
			continue
		}
		namets := int64(0)
		if namets, err = strconv.ParseInt(names[i][:len(names[i])-len(".valuestoc")], 10, 64); err != nil {
			vs.logError.Printf("bad timestamp in name: %#v\n", names[i])
			continue
		}
		if namets == 0 {
			vs.logError.Printf("bad timestamp in name: %#v\n", names[i])
			continue
		}
		vf := newValuesFile(vs, namets)
		fp, err := os.Open(path.Join(vs.pathtoc, names[i]))
		if err != nil {
			vs.logError.Printf("error opening %s: %s\n", names[i], err)
			continue
		}
		checksumFailures := 0
		first := true
		terminated := false
		fromDiskOverflow = fromDiskOverflow[:0]
		for {
			n, err := io.ReadFull(fp, fromDiskBuf)
			if n < 4 {
				if err != io.EOF && err != io.ErrUnexpectedEOF {
					vs.logError.Printf("error reading %s: %s\n", names[i], err)
				}
				break
			}
			n -= 4
			if murmur3.Sum32(fromDiskBuf[:n]) != binary.BigEndian.Uint32(fromDiskBuf[n:]) {
				checksumFailures++
			} else {
				j := 0
				if first {
					if !bytes.Equal(fromDiskBuf[:28], []byte("VALUESTORETOC v0            ")) {
						vs.logError.Printf("bad header: %s\n", names[i])
						break
					}
					if binary.BigEndian.Uint32(fromDiskBuf[28:]) != vs.checksumInterval {
						vs.logError.Printf("bad header checksum interval: %s\n", names[i])
						break
					}
					j += 32
					first = false
				}
				if n < int(vs.checksumInterval) {
					if binary.BigEndian.Uint32(fromDiskBuf[n-16:]) != 0 {
						vs.logError.Printf("bad terminator size marker: %s\n", names[i])
						break
					}
					if !bytes.Equal(fromDiskBuf[n-4:n], []byte("TERM")) {
						vs.logError.Printf("bad terminator: %s\n", names[i])
						break
					}
					n -= 16
					terminated = true
				}
				if len(fromDiskOverflow) > 0 {
					j += 32 - len(fromDiskOverflow)
					fromDiskOverflow = append(fromDiskOverflow, fromDiskBuf[j-32+len(fromDiskOverflow):j]...)
					keyB := binary.BigEndian.Uint64(fromDiskOverflow[8:])
					k := keyB % workers
					if batches[k] == nil {
						batches[k] = <-freeBatchChans[k]
						batchesPos[k] = 0
					}
					wr := &batches[k][batchesPos[k]]
					wr.keyA = binary.BigEndian.Uint64(fromDiskOverflow)
					wr.keyB = keyB
					wr.timestampbits = binary.BigEndian.Uint64(fromDiskOverflow[16:])
					wr.blockID = vf.id
					wr.offset = binary.BigEndian.Uint32(fromDiskOverflow[24:])
					wr.length = binary.BigEndian.Uint32(fromDiskOverflow[28:])
					batchesPos[k]++
					if batchesPos[k] >= _GLH_RECOVERY_BATCH_SIZE {
						pendingBatchChans[k] <- batches[k]
						batches[k] = nil
					}
					fromDiskCount++
					fromDiskOverflow = fromDiskOverflow[:0]
				}
				for ; j+32 <= n; j += 32 {
					keyB := binary.BigEndian.Uint64(fromDiskBuf[j+8:])
					k := keyB % workers
					if batches[k] == nil {
						batches[k] = <-freeBatchChans[k]
						batchesPos[k] = 0
					}
					wr := &batches[k][batchesPos[k]]
					wr.keyA = binary.BigEndian.Uint64(fromDiskBuf[j:])
					wr.keyB = keyB
					wr.timestampbits = binary.BigEndian.Uint64(fromDiskBuf[j+16:])
					wr.blockID = vf.id
					wr.offset = binary.BigEndian.Uint32(fromDiskBuf[j+24:])
					wr.length = binary.BigEndian.Uint32(fromDiskBuf[j+28:])
					batchesPos[k]++
					if batchesPos[k] >= _GLH_RECOVERY_BATCH_SIZE {
						pendingBatchChans[k] <- batches[k]
						batches[k] = nil
					}
					fromDiskCount++
				}
				if j != n {
					fromDiskOverflow = fromDiskOverflow[:n-j]
					copy(fromDiskOverflow, fromDiskBuf[j:])
				}
			}
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				vs.logError.Printf("error reading %s: %s\n", names[i], err)
				break
			}
		}
		fp.Close()
		if !terminated {
			vs.logError.Printf("early end of file: %s\n", names[i])
		}
		if checksumFailures > 0 {
			vs.logWarning.Printf("%d checksum failures for %s\n", checksumFailures, names[i])
		}
	}
	for i := 0; i < len(batches); i++ {
		if batches[i] != nil {
			pendingBatchChans[i] <- batches[i][:batchesPos[i]]
		}
		pendingBatchChans[i] <- nil
	}
	wg.Wait()
	if vs.logDebug != nil {
		dur := time.Now().Sub(start)
		valueCount, valueLength, _ := vs.GatherStats(false)
		vs.logInfo.Printf("%d key locations loaded in %s, %.0f/s; %d caused change; %d resulting locations referencing %d bytes.\n", fromDiskCount, dur, float64(fromDiskCount)/(float64(dur)/float64(time.Second)), causedChangeCount, valueCount, valueLength)
	}
}
