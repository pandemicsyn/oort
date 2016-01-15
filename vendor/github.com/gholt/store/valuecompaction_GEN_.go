package store

import (
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type valueCompactionState struct {
	interval       int
	threshold      float64
	ageThreshold   int64
	workerCount    int
	notifyChanLock sync.Mutex
	notifyChan     chan *bgNotification
}

func (store *DefaultValueStore) compactionConfig(cfg *ValueStoreConfig) {
	store.compactionState.interval = cfg.CompactionInterval
	store.compactionState.threshold = cfg.CompactionThreshold
	store.compactionState.ageThreshold = int64(cfg.CompactionAgeThreshold * 1000000000)
	store.compactionState.workerCount = cfg.CompactionWorkers
}

// CompactionPass will immediately execute a compaction pass to compact stale
// files.
func (store *DefaultValueStore) CompactionPass() {
	store.compactionState.notifyChanLock.Lock()
	if store.compactionState.notifyChan == nil {
		store.compactionPass(make(chan *bgNotification))
	} else {
		c := make(chan struct{}, 1)
		store.compactionState.notifyChan <- &bgNotification{
			action:   _BG_PASS,
			doneChan: c,
		}
		<-c
	}
	store.compactionState.notifyChanLock.Unlock()
}

// EnableCompaction will resume compaction passes. A compaction pass searches
// for files with a percentage of XX deleted entries.
func (store *DefaultValueStore) EnableCompaction() {
	store.compactionState.notifyChanLock.Lock()
	if store.compactionState.notifyChan == nil {
		store.compactionState.notifyChan = make(chan *bgNotification, 1)
		go store.compactionLauncher(store.compactionState.notifyChan)
	}
	store.compactionState.notifyChanLock.Unlock()
}

// DisableCompaction will stop any compaction passes until EnableCompaction is
// called. A compaction pass searches for files with a percentage of XX deleted
// entries.
func (store *DefaultValueStore) DisableCompaction() {
	store.compactionState.notifyChanLock.Lock()
	if store.compactionState.notifyChan != nil {
		c := make(chan struct{}, 1)
		store.compactionState.notifyChan <- &bgNotification{
			action:   _BG_DISABLE,
			doneChan: c,
		}
		<-c
		store.compactionState.notifyChan = nil
	}
	store.compactionState.notifyChanLock.Unlock()
}

func (store *DefaultValueStore) compactionLauncher(notifyChan chan *bgNotification) {
	interval := float64(store.compactionState.interval) * float64(time.Second)
	store.randMutex.Lock()
	nextRun := time.Now().Add(time.Duration(interval + interval*store.rand.NormFloat64()*0.1))
	store.randMutex.Unlock()
	var notification *bgNotification
	running := true
	for running {
		if notification == nil {
			sleep := nextRun.Sub(time.Now())
			if sleep > 0 {
				select {
				case notification = <-notifyChan:
				case <-time.After(sleep):
				}
			} else {
				select {
				case notification = <-notifyChan:
				default:
				}
			}
		}
		store.randMutex.Lock()
		nextRun = time.Now().Add(time.Duration(interval + interval*store.rand.NormFloat64()*0.1))
		store.randMutex.Unlock()
		if notification != nil {
			var nextNotification *bgNotification
			switch notification.action {
			case _BG_PASS:
				nextNotification = store.compactionPass(notifyChan)
			case _BG_DISABLE:
				running = false
			default:
				// Critical because there was a coding error that needs to be
				// fixed by a person.
				store.logCritical("compaction: invalid action requested: %d", notification.action)
			}
			notification.doneChan <- struct{}{}
			notification = nextNotification
		} else {
			notification = store.compactionPass(notifyChan)
		}
	}
}

type valueCompactionJob struct {
	fullPath         string
	candidateBlockID uint32
}

func (store *DefaultValueStore) compactionPass(notifyChan chan *bgNotification) *bgNotification {
	if store.logDebug != nil {
		begin := time.Now()
		defer func() {
			store.logDebug("compaction: pass took %s", time.Now().Sub(begin))
		}()
	}
	fp, err := os.Open(store.pathtoc)
	if err != nil {
		store.logError("compaction: %s", err)
		return nil
	}
	names, err := fp.Readdirnames(-1)
	fp.Close()
	if err != nil {
		store.logError("compaction: %s", err)
		return nil
	}
	sort.Strings(names)
	jobChan := make(chan *valueCompactionJob, len(names))
	controlChan := make(chan struct{})
	wg := &sync.WaitGroup{}
	for i := 0; i < store.compactionState.workerCount; i++ {
		wg.Add(1)
		go store.compactionWorker(jobChan, controlChan, wg)
	}
	waitChan := make(chan struct{}, 1)
	go func() {
		wg.Wait()
		close(waitChan)
	}()
	for _, name := range names {
		select {
		case notification := <-notifyChan:
			close(controlChan)
			<-waitChan
			return notification
		default:
		}
		if namets, valid := store.compactionCandidate(name); valid {
			jobChan <- &valueCompactionJob{path.Join(store.pathtoc, name), store.locBlockIDFromTimestampnano(namets)}
		}
	}
	close(jobChan)
	for {
		select {
		case notification := <-notifyChan:
			close(controlChan)
			<-waitChan
			return notification
		case <-waitChan:
			return nil
		}
	}
}

// compactionCandidate verifies that the given toc is a valid candidate for
// compaction and also returns the extracted namets.
func (store *DefaultValueStore) compactionCandidate(name string) (int64, bool) {
	if !strings.HasSuffix(name, ".valuetoc") {
		return 0, false
	}
	var namets int64
	_, n := path.Split(name)
	namets, err := strconv.ParseInt(n[:len(n)-len(".valuetoc")], 10, 64)
	if err != nil {
		store.logError("compaction: bad timestamp in name: %#v", name)
		return 0, false
	}
	if namets == 0 {
		store.logError("compaction: bad timestamp in name: %#v", name)
		return namets, false
	}
	if namets == int64(atomic.LoadUint64(&store.activeTOCA)) || namets == int64(atomic.LoadUint64(&store.activeTOCB)) {
		return namets, false
	}
	if namets >= time.Now().UnixNano()-store.compactionState.ageThreshold {
		return namets, false
	}
	return namets, true
}

func (store *DefaultValueStore) compactionWorker(jobChan chan *valueCompactionJob, controlChan chan struct{}, wg *sync.WaitGroup) {
	for c := range jobChan {
		select {
		case <-controlChan:
			break
		default:
		}
		total, err := valueTOCStat(c.fullPath, os.Stat, osOpenReadSeeker)
		if err != nil {
			store.logError("compaction: unable to stat %s because: %v", c.fullPath, err)
			continue
		}
		// TODO: This 1000 should be in the Config.
		// If total is less than 100, it'll automatically get compacted.
		if total < 1000 {
			atomic.AddInt32(&store.smallFileCompactions, 1)
		} else {
			toCheck := uint32(total)
			// If there are more than a million entries, we'll just check the
			// first million and extrapolate.
			if toCheck > 1000000 {
				toCheck = 1000000
			}
			checked, stale, err := store.sampleTOC(c.fullPath, c.candidateBlockID, toCheck)
			if err != nil {
				store.logError("compaction: unable to sample %s: %s", c.fullPath, err)
				continue
			}
			if store.logDebug != nil {
				store.logDebug("compaction: sample result: %s had %d entries; checked %d entries, %d were stale", c.fullPath, total, checked, stale)
			}
			if stale <= uint32(float64(checked)*store.compactionState.threshold) {
				continue
			}
			atomic.AddInt32(&store.compactions, 1)
		}
		store.compactFile(c.fullPath, c.candidateBlockID, controlChan)
	}
	wg.Done()
}

func (store *DefaultValueStore) sampleTOC(fullPath string, candidateBlockID uint32, toCheck uint32) (uint32, uint32, error) {
	stale := uint32(0)
	checked := uint32(0)
	// Compaction workers work on one file each; maybe we'll expand the workers
	// under a compaction worker sometime, but for now, limit it.
	workers := uint64(1)
	pendingBatchChans := make([]chan []valueTOCEntry, workers)
	freeBatchChans := make([]chan []valueTOCEntry, len(pendingBatchChans))
	for i := 0; i < len(pendingBatchChans); i++ {
		pendingBatchChans[i] = make(chan []valueTOCEntry, 3)
		freeBatchChans[i] = make(chan []valueTOCEntry, cap(pendingBatchChans[i]))
		for j := 0; j < cap(freeBatchChans[i]); j++ {
			freeBatchChans[i] <- make([]valueTOCEntry, store.recoveryBatchSize)
		}
	}
	controlChan := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(len(pendingBatchChans))
	for i := 0; i < len(pendingBatchChans); i++ {
		go func(pendingBatchChan chan []valueTOCEntry, freeBatchChan chan []valueTOCEntry) {
			skipRest := false
			for {
				batch := <-pendingBatchChan
				if batch == nil {
					break
				}
				if skipRest {
					continue
				}
				for j := 0; j < len(batch); j++ {
					wr := &batch[j]
					timestampBits, blockID, _, _ := store.lookup(wr.KeyA, wr.KeyB)
					if timestampBits != wr.TimestampBits || blockID != wr.BlockID {
						atomic.AddUint32(&stale, 1)
					}
					if c := atomic.AddUint32(&checked, 1); c == toCheck {
						skipRest = true
						close(controlChan)
						break
					} else if c > toCheck {
						skipRest = true
						break
					}
				}
				freeBatchChan <- batch
			}
			wg.Done()
		}(pendingBatchChans[i], freeBatchChans[i])
	}
	fpr, err := osOpenReadSeeker(fullPath)
	if err != nil {
		return 0, 0, err
	}
	_, errs := valueReadTOCEntriesBatched(fpr, candidateBlockID, freeBatchChans, pendingBatchChans, controlChan)
	for _, err := range errs {
		store.logError("compaction: check error with %s: %s", fullPath, err)
		// TODO: The auditor should catch this eventually, but we should be
		// proactive and notify the auditor of the issue here.
	}
	closeIfCloser(fpr)
	for i := 0; i < len(pendingBatchChans); i++ {
		pendingBatchChans[i] <- nil
	}
	wg.Wait()
	return checked, stale, nil
}

func (store *DefaultValueStore) compactFile(fullPath string, blockID uint32, controlChan chan struct{}) {
	// TODO: Compaction needs to rewrite all the good entries it can, but also
	// deliberately remove any known bad entries from the locmap so that
	// replication can get them back in place from other servers.
	var errorCount uint32
	var count uint32
	var rewrote uint32
	var stale uint32
	// Compaction workers work on one file each; maybe we'll expand the workers
	// under a compaction worker sometime, but for now, limit it.
	workers := uint64(1)
	pendingBatchChans := make([]chan []valueTOCEntry, workers)
	freeBatchChans := make([]chan []valueTOCEntry, len(pendingBatchChans))
	for i := 0; i < len(pendingBatchChans); i++ {
		pendingBatchChans[i] = make(chan []valueTOCEntry, 3)
		freeBatchChans[i] = make(chan []valueTOCEntry, cap(pendingBatchChans[i]))
		for j := 0; j < cap(freeBatchChans[i]); j++ {
			freeBatchChans[i] <- make([]valueTOCEntry, store.recoveryBatchSize)
		}
	}
	wg := &sync.WaitGroup{}
	wg.Add(len(pendingBatchChans))
	for i := 0; i < len(pendingBatchChans); i++ {
		go func(pendingBatchChan chan []valueTOCEntry, freeBatchChan chan []valueTOCEntry) {
			var value []byte
			for {
				batch := <-pendingBatchChan
				if batch == nil {
					break
				}
				if atomic.LoadUint32(&errorCount) > 0 {
					continue
				}
				for j := 0; j < len(batch); j++ {
					atomic.AddUint32(&count, 1)
					wr := &batch[j]
					timestampBits, _, _, _ := store.lookup(wr.KeyA, wr.KeyB)
					if timestampBits > wr.TimestampBits {
						atomic.AddUint32(&stale, 1)
						continue
					}
					timestampBits, value, err := store.read(wr.KeyA, wr.KeyB, value[:0])
					if timestampBits > wr.TimestampBits {
						atomic.AddUint32(&stale, 1)
						continue
					}
					_, err = store.write(wr.KeyA, wr.KeyB, wr.TimestampBits|_TSB_COMPACTION_REWRITE, value, true)
					if err != nil {
						store.logError("compactFile: error with %s: %s", fullPath, err)
						atomic.AddUint32(&errorCount, 1)
						break
					}
					atomic.AddUint32(&rewrote, 1)
				}
				freeBatchChan <- batch
			}
			wg.Done()
		}(pendingBatchChans[i], freeBatchChans[i])
	}
	spindown := func(remove bool) {
		for i := 0; i < len(pendingBatchChans); i++ {
			pendingBatchChans[i] <- nil
		}
		wg.Wait()
		if remove {
			if err := os.Remove(fullPath); err != nil {
				store.logError("compactFile: unable to remove %s %s", fullPath, err)
				if err = os.Rename(fullPath, fullPath+".renamed"); err != nil {
					// Critical level since future recoveries, compactions, and
					// audits will keep hitting this file until a person
					// corrects the file system issue.
					store.logCritical("compactFile: also could not rename %s %s", fullPath, err)
				}
			}
			if err := os.Remove(fullPath[:len(fullPath)-len("toc")]); err != nil {
				store.logError("compactFile: unable to remove %s %s", fullPath[:len(fullPath)-len("toc")], err)
				if err = os.Rename(fullPath[:len(fullPath)-len("toc")], fullPath[:len(fullPath)-len("toc")]+".renamed"); err != nil {
					store.logError("compactFile: also count not rename %s %s", fullPath[:len(fullPath)-len("toc")], err)
				}
			}
			if blockID != 0 {
				if err := store.closeLocBlock(blockID); err != nil {
					store.logError("compactFile: error closing in-memory block for %s: %s", fullPath, err)
				}
			}
		}
		if store.logDebug != nil {
			store.logDebug("compactFile: %s (total %d, rewrote %d, stale %d)", fullPath, atomic.LoadUint32(&count), atomic.LoadUint32(&rewrote), atomic.LoadUint32(&stale))
		}
	}
	fpr, err := osOpenReadSeeker(fullPath)
	if err != nil {
		store.logError("compactFile: error opening %s: %s", fullPath, err)
		spindown(false)
		return
	}
	fdc, errs := valueReadTOCEntriesBatched(fpr, blockID, freeBatchChans, pendingBatchChans, controlChan)
	closeIfCloser(fpr)
	for _, err := range errs {
		store.logError("compactFile: error with %s: %s", fullPath, err)
	}
	select {
	case <-controlChan:
		if store.logDebug != nil {
			store.logDebug("compactFile: canceled compaction of %s.", fullPath)
		}
		spindown(false)
		return
	default:
	}
	if len(errs) > 0 {
		if fdc == 0 {
			store.logError("compactFile: errors with %s and no entries were read; file will be retried later.", fullPath)
			spindown(false)
			return
		} else {
			store.logError("compactFile: errors with %s but some entries were read; assuming the recovery was as good as it could get and removing file.", fullPath)
		}
	}
	spindown(true)
}
