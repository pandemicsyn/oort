package valuestore

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/gholt/brimtime.v1"
)

const _GLH_TOMBSTONE_DISCARD_BATCH_SIZE = 1024 * 1024

type tombstoneDiscardState struct {
	interval      int
	age           uint64
	notifyChan    chan *backgroundNotification
	abort         uint32
	localRemovals [][]localRemovalEntry
}

type localRemovalEntry struct {
	keyA          uint64
	keyB          uint64
	timestampbits uint64
}

func (vs *DefaultValueStore) tombstoneDiscardInit(cfg *config) {
	vs.tombstoneDiscardState.interval = cfg.tombstoneDiscardInterval
	vs.tombstoneDiscardState.age = (uint64(cfg.tombstoneAge) * uint64(time.Second) / 1000) << _TSB_UTIL_BITS
	vs.tombstoneDiscardState.notifyChan = make(chan *backgroundNotification, 1)
	go vs.tombstoneDiscardLauncher()
}

// DisableTombstoneDiscard will stop any discard passes until
// EnableTombstoneDiscard is called. A discard pass removes expired tombstones
// (deletion markers).
func (vs *DefaultValueStore) DisableTombstoneDiscard() {
	c := make(chan struct{}, 1)
	vs.tombstoneDiscardState.notifyChan <- &backgroundNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableTombstoneDiscard will resume discard passes. A discard pass removes
// expired tombstones (deletion markers).
func (vs *DefaultValueStore) EnableTombstoneDiscard() {
	c := make(chan struct{}, 1)
	vs.tombstoneDiscardState.notifyChan <- &backgroundNotification{
		enable:   true,
		doneChan: c,
	}
	<-c
}

// TombstoneDiscardPass will immediately execute a pass to discard expired
// tombstones (deletion markers) rather than waiting for the next interval. If
// a pass is currently executing, it will be stopped and restarted so that a
// call to this function ensures one complete pass occurs.
func (vs *DefaultValueStore) TombstoneDiscardPass() {
	atomic.StoreUint32(&vs.tombstoneDiscardState.abort, 1)
	c := make(chan struct{}, 1)
	vs.tombstoneDiscardState.notifyChan <- &backgroundNotification{doneChan: c}
	<-c
}

func (vs *DefaultValueStore) tombstoneDiscardLauncher() {
	var enabled bool
	interval := float64(vs.tombstoneDiscardState.interval) * float64(time.Second)
	nextRun := time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
	for {
		var notification *backgroundNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-vs.tombstoneDiscardState.notifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-vs.tombstoneDiscardState.notifyChan:
			default:
			}
		}
		nextRun = time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
		if notification != nil {
			if notification.enable {
				enabled = true
				notification.doneChan <- struct{}{}
				continue
			}
			if notification.disable {
				atomic.StoreUint32(&vs.tombstoneDiscardState.abort, 1)
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&vs.tombstoneDiscardState.abort, 0)
			vs.tombstoneDiscardPass()
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&vs.tombstoneDiscardState.abort, 0)
			vs.tombstoneDiscardPass()
		}
	}
}

func (vs *DefaultValueStore) tombstoneDiscardPass() {
	if vs.logDebug != nil {
		begin := time.Now()
		defer func() {
			vs.logDebug.Printf("tombstone discard pass took %s", time.Now().Sub(begin))
		}()
	}
	vs.tombstoneDiscardPassLocalRemovals()
	vs.tombstoneDiscardPassExpiredDeletions()
}

// tombstoneDiscardPassLocalRemovals removes all valuelocmap entries marked
// with the _TSB_LOCAL_REMOVAL bit. These are entries that other routines have
// indicated are no longer needed in memory.
func (vs *DefaultValueStore) tombstoneDiscardPassLocalRemovals() {
	// Each worker will perform a pass on a subsection of each partition's key
	// space. Additionally, each worker will start their work on different
	// partition. This reduces contention for a given section of the
	// valuelocmap.
	partitionShift := uint16(0)
	partitionMax := uint64(0)
	if vs.msgRing != nil {
		pbc := vs.msgRing.Ring().PartitionBitCount()
		partitionShift = 64 - pbc
		partitionMax = (uint64(1) << pbc) - 1
	}
	workerMax := uint64(vs.workers - 1)
	workerPartitionPiece := (uint64(1) << partitionShift) / (workerMax + 1)
	work := func(partition uint64, worker uint64) {
		partitionOnLeftBits := partition << partitionShift
		rangeBegin := partitionOnLeftBits + (workerPartitionPiece * worker)
		var rangeEnd uint64
		// A little bit of complexity here to handle where the more general
		// expressions would have overflow issues.
		if worker != workerMax {
			rangeEnd = partitionOnLeftBits + (workerPartitionPiece * (worker + 1)) - 1
		} else {
			if partition != partitionMax {
				rangeEnd = ((partition + 1) << partitionShift) - 1
			} else {
				rangeEnd = math.MaxUint64
			}
		}
		vs.vlm.Discard(rangeBegin, rangeEnd, _TSB_LOCAL_REMOVAL)
	}
	wg := &sync.WaitGroup{}
	wg.Add(int(workerMax + 1))
	workerPartitionOffset := (partitionMax + 1) / (workerMax + 1)
	for worker := uint64(0); worker <= workerMax; worker++ {
		go func(worker uint64) {
			partitionBegin := workerPartitionOffset * worker
			for partition := partitionBegin; partition <= partitionMax; partition++ {
				work(partition, worker)
			}
			for partition := uint64(0); partition < partitionBegin; partition++ {
				work(partition, worker)
			}
			wg.Done()
		}(worker)
	}
	wg.Wait()
}

// tombstoneDiscardPassExpiredDeletions scans for valuelocmap entries marked
// with _TSB_DELETION (but not _TSB_LOCAL_REMOVAL) that are older than the
// maximum tombstone age and marks them for _TSB_LOCAL_REMOVAL.
func (vs *DefaultValueStore) tombstoneDiscardPassExpiredDeletions() {
	// Each worker will perform a pass on a subsection of each partition's key
	// space. Additionally, each worker will start their work on different
	// partition. This reduces contention for a given section of the
	// valuelocmap.
	partitionShift := uint16(0)
	partitionMax := uint64(0)
	if vs.msgRing != nil {
		pbc := vs.msgRing.Ring().PartitionBitCount()
		partitionShift = 64 - pbc
		partitionMax = (uint64(1) << pbc) - 1
	}
	workerMax := uint64(vs.workers - 1)
	workerPartitionPiece := (uint64(1) << partitionShift) / (workerMax + 1)
	work := func(partition uint64, worker uint64, localRemovals []localRemovalEntry) {
		partitionOnLeftBits := partition << partitionShift
		rangeBegin := partitionOnLeftBits + (workerPartitionPiece * worker)
		var rangeEnd uint64
		// A little bit of complexity here to handle where the more general
		// expressions would have overflow issues.
		if worker != workerMax {
			rangeEnd = partitionOnLeftBits + (workerPartitionPiece * (worker + 1)) - 1
		} else {
			if partition != partitionMax {
				rangeEnd = ((partition + 1) << partitionShift) - 1
			} else {
				rangeEnd = math.MaxUint64
			}
		}
		cutoff := (uint64(brimtime.TimeToUnixMicro(time.Now())) << _TSB_UTIL_BITS) - vs.tombstoneDiscardState.age
		more := true
		for more {
			localRemovalsIndex := 0
			// Since we shouldn't try to modify what we're scanning while we're
			// scanning (lock contention) we instead record in localRemovals
			// what to modify after the scan.
			rangeBegin, more = vs.vlm.ScanCallback(rangeBegin, rangeEnd, _TSB_DELETION, _TSB_LOCAL_REMOVAL, cutoff, _GLH_TOMBSTONE_DISCARD_BATCH_SIZE, func(keyA uint64, keyB uint64, timestampbits uint64, length uint32) {
				e := &localRemovals[localRemovalsIndex]
				e.keyA = keyA
				e.keyB = keyB
				e.timestampbits = timestampbits
				localRemovalsIndex++
			})
			for i := 0; i < localRemovalsIndex; i++ {
				e := &localRemovals[i]
				// These writes go through the entire system, so they're
				// persisted and therefore restored on restarts.
				vs.write(e.keyA, e.keyB, e.timestampbits|_TSB_LOCAL_REMOVAL, nil)
			}
		}
	}
	// To avoid memory churn, the localRemovals scratchpads are allocated just
	// once and passed in to the workers.
	for len(vs.tombstoneDiscardState.localRemovals) <= int(workerMax) {
		vs.tombstoneDiscardState.localRemovals = append(vs.tombstoneDiscardState.localRemovals, make([]localRemovalEntry, _GLH_TOMBSTONE_DISCARD_BATCH_SIZE))
	}
	wg := &sync.WaitGroup{}
	wg.Add(int(workerMax + 1))
	for worker := uint64(0); worker <= workerMax; worker++ {
		go func(worker uint64) {
			localRemovals := vs.tombstoneDiscardState.localRemovals[worker]
			partitionBegin := (partitionMax + 1) / (workerMax + 1) * worker
			for partition := partitionBegin; ; {
				work(partition, worker, localRemovals)
				partition++
				if partition == partitionBegin {
					break
				}
				if partition > partitionMax {
					partition = 0
				}
			}
			wg.Done()
		}(worker)
	}
	wg.Wait()
}
