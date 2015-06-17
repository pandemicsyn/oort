package valuestore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/spaolacci/murmur3"
)

type compactionState struct {
	interval     int
	workerCount  int
	ageThreshold int64
	abort        uint32
	threshold    float64
	notifyChan   chan *backgroundNotification
}

func (vs *DefaultValueStore) compactionInit(cfg *config) {
	vs.compactionState.interval = cfg.compactionInterval
	vs.compactionState.threshold = cfg.compactionThreshold
	vs.compactionState.ageThreshold = int64(cfg.compactionAgeThreshold * 1000000000)
	vs.compactionState.notifyChan = make(chan *backgroundNotification, 1)
	vs.compactionState.workerCount = cfg.compactionWorkers
	go vs.compactionLauncher()
}

// DisableCompaction will stop any compaction passes until
// EnableCompaction is called. A compaction pass searches for files
// with a percentage of XX deleted entries.
func (vs *DefaultValueStore) DisableCompaction() {
	c := make(chan struct{}, 1)
	vs.compactionState.notifyChan <- &backgroundNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableCompaction will resume compaction passes.
// A compaction pass searches for files with a percentage of XX deleted
// entries.
func (vs *DefaultValueStore) EnableCompaction() {
	c := make(chan struct{}, 1)
	vs.compactionState.notifyChan <- &backgroundNotification{
		enable:   true,
		doneChan: c,
	}
	<-c
}

// CompactionPass will immediately execute a compaction pass to compact stale files.
func (vs *DefaultValueStore) CompactionPass() {
	atomic.StoreUint32(&vs.compactionState.abort, 1)
	c := make(chan struct{}, 1)
	vs.compactionState.notifyChan <- &backgroundNotification{doneChan: c}
	<-c
}

func (vs *DefaultValueStore) compactionLauncher() {
	var enabled bool
	interval := float64(vs.compactionState.interval) * float64(time.Second)
	nextRun := time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
	for {
		var notification *backgroundNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-vs.compactionState.notifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-vs.compactionState.notifyChan:
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
				atomic.StoreUint32(&vs.compactionState.abort, 1)
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&vs.compactionState.abort, 0)
			vs.compactionPass()
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&vs.compactionState.abort, 0)
			vs.compactionPass()
		}
	}
}

type compactionJob struct {
	name             string
	candidateBlockID uint32
}

func (vs *DefaultValueStore) compactionPass() {
	if vs.logDebug != nil {
		begin := time.Now()
		defer func() {
			vs.logDebug.Printf("compaction pass took %s", time.Now().Sub(begin))
		}()
	}
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

	compactionJobs := make(chan compactionJob, len(names))
	compactionResults := make(chan string, len(names))

	//Spin up new workers on each pass rather than at startup so that
	//the number of workers can change between passes.
	for i := 1; i <= vs.compactionState.workerCount; i++ {
		go vs.compactionWorker(i, compactionJobs, compactionResults)
	}

	submitted := 0
	for i := 0; i < len(names); i++ {
		namets, valid := vs.compactionCandidate(names[i])
		if valid {
			compactionJobs <- compactionJob{names[i], vs.valueLocBlockIDFromTimestampnano(namets)}
			submitted++
		}
	}
	close(compactionJobs)
	if vs.logDebug != nil {
		vs.logDebug.Println("compaction candidates submitted:", submitted)
	}
	for i := 1; i <= submitted; i++ {
		<-compactionResults
	}
	close(compactionResults)
}

// compactionCandidate verifies that the given toc is a valid candidate for
// compaction and also returns the extracted namets.
// TODO: This doesn't need to be its own func anymore
func (vs *DefaultValueStore) compactionCandidate(name string) (int64, bool) {
	if !strings.HasSuffix(name, ".valuestoc") {
		return 0, false
	}
	var namets int64
	namets, err := strconv.ParseInt(name[:len(name)-len(".valuestoc")], 10, 64)
	if err != nil {
		vs.logError.Printf("bad timestamp in name: %#v\n", name)
		return 0, false
	}
	if namets == 0 {
		vs.logError.Printf("bad timestamp in name: %#v\n", name)
		return namets, false
	}
	if namets == int64(atomic.LoadUint64(&vs.activeTOCA)) || namets == int64(atomic.LoadUint64(&vs.activeTOCB)) {
		return namets, false
	}
	if namets >= time.Now().UnixNano()-vs.compactionState.ageThreshold {
		return namets, false
	}
	return namets, true
}

func (vs *DefaultValueStore) compactionWorker(id int, tocfiles <-chan compactionJob, result chan<- string) {
	for c := range tocfiles {
		f, err := os.Open(c.name)
		if err != nil {
			vs.logError.Println("Unable to open for stat:", c.name)
			continue
		}
		fstat, err := f.Stat()
		if err != nil {
			vs.logError.Println("Unable to stat:", c.name)
			continue
		}
		total := int(fstat.Size()) / 34
		if total < 100 {
			vs.logInfo.Println("Triggering compaction for", c.name, "due to size")
			result, err := vs.compactFile(c.name, c.candidateBlockID)
			if err != nil {
				vs.logCritical.Println(err.Error())
			}
			if (result.rewrote + result.stale) == result.count {
				err = os.Remove(c.name)
				if err != nil {
					vs.logCritical.Println("Unable to remove", c.name, err.Error())
					continue
				}
				err = os.Remove(c.name[:len(c.name)-len("toc")])
				if err != nil {
					vs.logCritical.Println("Unable to remove", c.name, "values", err.Error())
					continue
				}
				vs.logInfo.Printf("Compacted %s (total %d, rewrote %d, stale %d)", c.name, result.count, result.rewrote, result.stale)
			}
		} else {
			rand.Seed(time.Now().UnixNano())
			skipOffset := rand.Intn(int(float64(total) * 0.01)) //randomly skip up to the first 1% of entries
			skipTotal := total - skipOffset
			staleTarget := int(float64(skipTotal) * vs.compactionState.threshold)
			skip := skipTotal/staleTarget - 1
			count, stale, err := vs.sampleTOC(c.name, c.candidateBlockID, skipOffset, skip)
			if err != nil {
				continue
			}
			if vs.logDebug != nil {
				vs.logDebug.Println(c.name, "sample result:", count, stale, staleTarget)
			}
			if stale >= staleTarget {
				if vs.logDebug != nil {
					vs.logDebug.Println("Triggering compaction for", c.name, "with", count, "entries.")
				}
				result, err := vs.compactFile(c.name, c.candidateBlockID)
				if err != nil {
					vs.logCritical.Println(err.Error())
				}
				if (result.rewrote + result.stale) == result.count {
					err = os.Remove(c.name)
					if err != nil {
						vs.logCritical.Println("Unable to remove", c.name, err.Error())
						continue
					}
					err = os.Remove(c.name[:len(c.name)-len("toc")])
					if err != nil {
						vs.logCritical.Println("Unable to remove", c.name, "values", err.Error())
						continue
					}
					vs.logInfo.Printf("Compacted %s: (total %d, rewrote %d, stale %d)", c.name, result.count, result.rewrote, result.stale)
				}
			}
		}
		result <- c.name
	}
}

func (vs *DefaultValueStore) sampleTOC(name string, candidateBlockID uint32, skipOffset, skipCount int) (int, int, error) {
	count := 0
	stale := 0
	fromDiskBuf := make([]byte, vs.checksumInterval+4)
	fromDiskOverflow := make([]byte, 0, 32)
	fp, err := os.Open(path.Join(vs.pathtoc, name))
	if err != nil {
		vs.logError.Printf("error opening %s: %s\n", name, err)
		return 0, 0, err
	}
	checksumFailures := 0
	first := true
	terminated := false
	fromDiskOverflow = fromDiskOverflow[:0]
	skipCounter := 0 - skipOffset
	for {
		n, err := io.ReadFull(fp, fromDiskBuf)
		if n < 4 {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				vs.logError.Printf("error reading %s: %s\n", name, err)
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
					vs.logError.Printf("bad header: %s\n", name)
					break
				}
				if binary.BigEndian.Uint32(fromDiskBuf[28:]) != vs.checksumInterval {
					vs.logError.Printf("bad header checksum interval: %s\n", name)
					break
				}
				j += 32
				first = false
			}
			if n < int(vs.checksumInterval) {
				if binary.BigEndian.Uint32(fromDiskBuf[n-16:]) != 0 {
					vs.logError.Printf("bad terminator size marker: %s\n", name)
					break
				}
				if !bytes.Equal(fromDiskBuf[n-4:n], []byte("TERM")) {
					vs.logError.Printf("bad terminator: %s\n", name)
					break
				}
				n -= 16
				terminated = true
			}
			if len(fromDiskOverflow) > 0 {
				j += 32 - len(fromDiskOverflow)
				fromDiskOverflow = append(fromDiskOverflow, fromDiskBuf[j-32+len(fromDiskOverflow):j]...)
				keyB := binary.BigEndian.Uint64(fromDiskOverflow[8:])
				keyA := binary.BigEndian.Uint64(fromDiskOverflow)
				timestampbits := binary.BigEndian.Uint64(fromDiskOverflow[16:])
				fromDiskOverflow = fromDiskOverflow[:0]
				count++
				if skipCounter == skipCount {
					tsm, blockid, _, _ := vs.lookup(keyA, keyB)
					if tsm>>_TSB_UTIL_BITS != timestampbits>>_TSB_UTIL_BITS && blockid != candidateBlockID || tsm&_TSB_DELETION != 0 {
						stale++
					}
					skipCounter = 0
				} else {
					skipCounter++
				}

			}
			for ; j+32 <= n; j += 32 {
				keyB := binary.BigEndian.Uint64(fromDiskBuf[j+8:])
				keyA := binary.BigEndian.Uint64(fromDiskBuf[j:])
				timestampbits := binary.BigEndian.Uint64(fromDiskBuf[j+16:])
				tsm, blockid, _, _ := vs.lookup(keyA, keyB)
				count++
				if skipCounter == skipCount {
					if tsm>>_TSB_UTIL_BITS != timestampbits>>_TSB_UTIL_BITS && blockid != candidateBlockID || tsm&_TSB_DELETION != 0 {
						stale++
					}
					skipCounter = 0
				} else {
					skipCounter++
				}
			}
			if j != n {
				fromDiskOverflow = fromDiskOverflow[:n-j]
				copy(fromDiskOverflow, fromDiskBuf[j:])
			}
		}
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			vs.logError.Printf("error reading %s: %s\n", name, err)
			break
		}
	}
	fp.Close()
	if !terminated {
		vs.logError.Printf("early end of file: %s\n", name)
	}
	if checksumFailures > 0 {
		vs.logWarning.Printf("%d checksum failures for %s\n", checksumFailures, name)
	}
	return count, stale, nil

}

type compactionResult struct {
	checksumFailures int
	count            int
	rewrote          int
	stale            int
}

func (vs *DefaultValueStore) compactFile(name string, candidateBlockID uint32) (compactionResult, error) {
	var cr compactionResult
	fromDiskBuf := make([]byte, vs.checksumInterval+4)
	fromDiskOverflow := make([]byte, 0, 32)
	fp, err := os.Open(path.Join(vs.pathtoc, name))
	if err != nil {
		vs.logError.Printf("error opening %s: %s\n", name, err)
		return cr, errors.New("Error opening toc")
	}
	first := true
	terminated := false
	fromDiskOverflow = fromDiskOverflow[:0]
	for {
		n, err := io.ReadFull(fp, fromDiskBuf)
		if n < 4 {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				vs.logError.Printf("error reading %s: %s\n", name, err)
				return cr, errors.New("Error attempting to read toc")
			}
			break
		}
		n -= 4
		if murmur3.Sum32(fromDiskBuf[:n]) != binary.BigEndian.Uint32(fromDiskBuf[n:]) {
			cr.checksumFailures++
		} else {
			j := 0
			if first {
				if !bytes.Equal(fromDiskBuf[:28], []byte("VALUESTORETOC v0            ")) {
					vs.logError.Printf("bad header: %s\n", name)
					return cr, errors.New("Bad header")
				}
				if binary.BigEndian.Uint32(fromDiskBuf[28:]) != vs.checksumInterval {
					vs.logError.Printf("bad header checksum interval: %s\n", name)
					return cr, errors.New("Bad header checksum interval")
				}
				j += 32
				first = false
			}
			if n < int(vs.checksumInterval) {
				if binary.BigEndian.Uint32(fromDiskBuf[n-16:]) != 0 {
					vs.logError.Printf("bad terminator size marker: %s\n", name)
					return cr, errors.New("Error on toc term size marker")
				}
				if !bytes.Equal(fromDiskBuf[n-4:n], []byte("TERM")) {
					vs.logError.Printf("bad terminator: %s\n", name)
					return cr, errors.New("Error on toc term marker")
				}
				n -= 16
				terminated = true
			}
			if len(fromDiskOverflow) > 0 {
				j += 32 - len(fromDiskOverflow)
				fromDiskOverflow = append(fromDiskOverflow, fromDiskBuf[j-32+len(fromDiskOverflow):j]...)
				keyB := binary.BigEndian.Uint64(fromDiskOverflow[8:])
				keyA := binary.BigEndian.Uint64(fromDiskOverflow)
				timestampbits := binary.BigEndian.Uint64(fromDiskOverflow[16:])
				fromDiskOverflow = fromDiskOverflow[:0]
				tsm, blockid, _, _ := vs.lookup(keyA, keyB)
				if tsm>>_TSB_UTIL_BITS != timestampbits>>_TSB_UTIL_BITS && blockid != candidateBlockID || tsm&_TSB_DELETION != 0 {
					cr.count++
					cr.stale++
				} else {
					var value []byte
					_, value, err := vs.read(keyA, keyB, value)
					if err != nil {
						vs.logCritical.Println("Error on rewrite read", err.Error())
						return cr, errors.New("Error on read for compaction rewrite.")
					}
					_, err = vs.write(keyA, keyB, timestampbits|_TSB_COMPACTION_REWRITE, value)
					if err != nil {
						vs.logCritical.Println("Error on rewrite", err.Error())
						return cr, errors.New("Write error on compaction rewrite.")
					}
					cr.count++
					cr.rewrote++
				}
			}
			for ; j+32 <= n; j += 32 {
				keyB := binary.BigEndian.Uint64(fromDiskBuf[j+8:])
				keyA := binary.BigEndian.Uint64(fromDiskBuf[j:])
				timestampbits := binary.BigEndian.Uint64(fromDiskBuf[j+16:])
				tsm, blockid, _, _ := vs.lookup(keyA, keyB)
				if tsm>>_TSB_UTIL_BITS != timestampbits>>_TSB_UTIL_BITS && blockid != candidateBlockID || tsm&_TSB_DELETION != 0 {
					cr.count++
					cr.stale++
				} else {
					var value []byte
					_, value, err := vs.read(keyA, keyB, value)
					if err != nil {
						vs.logCritical.Println("Error on rewrite read", err.Error())
						return cr, errors.New("Error on rewrite read")
					}
					_, err = vs.write(keyA, keyB, timestampbits|_TSB_COMPACTION_REWRITE, value)
					if err != nil {
						vs.logCritical.Println("Error on rewrite", err.Error())
						return cr, errors.New("Error on rewrite")
					}
					cr.count++
					cr.rewrote++
				}
			}
			if j != n {
				fromDiskOverflow = fromDiskOverflow[:n-j]
				copy(fromDiskOverflow, fromDiskBuf[j:])
			}
		}
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			vs.logError.Printf("error reading %s: %s\n", name, err)
			return cr, errors.New("EOF while reading toc during compaction")
		}
	}
	fp.Close()
	if !terminated {
		vs.logError.Printf("early end of file: %s\n", name)
		return cr, nil

	}
	if cr.checksumFailures > 0 {
		vs.logWarning.Printf("%d checksum failures for %s\n", cr.checksumFailures, name)
		return cr, nil

	}
	return cr, nil
}
