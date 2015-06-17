package valuestore

import (
	"fmt"
	"sync/atomic"
	"time"

	"gopkg.in/gholt/brimtext.v1"
)

type valueStoreStats struct {
	debug                      bool
	freeableVMChansCap         int
	freeableVMChansIn          int
	freeVMChanCap              int
	freeVMChanIn               int
	freeVWRChans               int
	freeVWRChansCap            int
	freeVWRChansIn             int
	pendingVWRChans            int
	pendingVWRChansCap         int
	pendingVWRChansIn          int
	vfVMChanCap                int
	vfVMChanIn                 int
	freeTOCBlockChanCap        int
	freeTOCBlockChanIn         int
	pendingTOCBlockChanCap     int
	pendingTOCBlockChanIn      int
	maxValueLocBlockID         uint64
	path                       string
	pathtoc                    string
	workers                    int
	tombstoneDiscardInterval   int
	outPullReplicationWorkers  uint64
	outPullReplicationInterval int
	outPushReplicationWorkers  int
	outPushReplicationInterval int
	maxValueSize               uint32
	pageSize                   uint32
	minValueAlloc              int
	writePagesPerWorker        int
	tombstoneAge               int
	valuesFileSize             uint32
	valuesFileReaders          int
	checksumInterval           uint32
	replicationIgnoreRecent    int
	vlmCount                   uint64
	vlmLength                  uint64
	vlmDebugInfo               fmt.Stringer
}

// GatherStats returns overall information about the state of the ValueStore.
func (vs *DefaultValueStore) GatherStats(debug bool) (uint64, uint64, fmt.Stringer) {
	stats := &valueStoreStats{}
	if debug {
		stats.debug = debug
		for i := 0; i < len(vs.freeableVMChans); i++ {
			stats.freeableVMChansCap += cap(vs.freeableVMChans[i])
			stats.freeableVMChansIn += len(vs.freeableVMChans[i])
		}
		stats.freeVMChanCap = cap(vs.freeVMChan)
		stats.freeVMChanIn = len(vs.freeVMChan)
		stats.freeVWRChans = len(vs.freeVWRChans)
		for i := 0; i < len(vs.freeVWRChans); i++ {
			stats.freeVWRChansCap += cap(vs.freeVWRChans[i])
			stats.freeVWRChansIn += len(vs.freeVWRChans[i])
		}
		stats.pendingVWRChans = len(vs.pendingVWRChans)
		for i := 0; i < len(vs.pendingVWRChans); i++ {
			stats.pendingVWRChansCap += cap(vs.pendingVWRChans[i])
			stats.pendingVWRChansIn += len(vs.pendingVWRChans[i])
		}
		stats.vfVMChanCap = cap(vs.vfVMChan)
		stats.vfVMChanIn = len(vs.vfVMChan)
		stats.freeTOCBlockChanCap = cap(vs.freeTOCBlockChan)
		stats.freeTOCBlockChanIn = len(vs.freeTOCBlockChan)
		stats.pendingTOCBlockChanCap = cap(vs.pendingTOCBlockChan)
		stats.pendingTOCBlockChanIn = len(vs.pendingTOCBlockChan)
		stats.maxValueLocBlockID = atomic.LoadUint64(&vs.valueLocBlockIDer)
		stats.path = vs.path
		stats.pathtoc = vs.pathtoc
		stats.workers = vs.workers
		stats.tombstoneDiscardInterval = vs.tombstoneDiscardState.interval
		stats.outPullReplicationWorkers = vs.pullReplicationState.outWorkers
		stats.outPullReplicationInterval = vs.pullReplicationState.outInterval
		stats.outPushReplicationWorkers = vs.pushReplicationState.outWorkers
		stats.outPushReplicationInterval = vs.pushReplicationState.outInterval
		stats.maxValueSize = vs.maxValueSize
		stats.pageSize = vs.pageSize
		stats.minValueAlloc = vs.minValueAlloc
		stats.writePagesPerWorker = vs.writePagesPerWorker
		stats.tombstoneAge = int((vs.tombstoneDiscardState.age >> _TSB_UTIL_BITS) * 1000 / uint64(time.Second))
		stats.valuesFileSize = vs.valuesFileSize
		stats.valuesFileReaders = vs.valuesFileReaders
		stats.checksumInterval = vs.checksumInterval
		stats.replicationIgnoreRecent = int(vs.replicationIgnoreRecent / uint64(time.Second))
		stats.vlmCount, stats.vlmLength, stats.vlmDebugInfo = vs.vlm.GatherStats(_TSB_INACTIVE, true)
	} else {
		stats.vlmCount, stats.vlmLength, stats.vlmDebugInfo = vs.vlm.GatherStats(_TSB_INACTIVE, false)
	}
	return stats.vlmCount, stats.vlmLength, stats
}

func (stats *valueStoreStats) String() string {
	if stats.debug {
		return brimtext.Align([][]string{
			[]string{"freeableVMChansCap", fmt.Sprintf("%d", stats.freeableVMChansCap)},
			[]string{"freeableVMChansIn", fmt.Sprintf("%d", stats.freeableVMChansIn)},
			[]string{"freeVMChanCap", fmt.Sprintf("%d", stats.freeVMChanCap)},
			[]string{"freeVMChanIn", fmt.Sprintf("%d", stats.freeVMChanIn)},
			[]string{"freeVWRChans", fmt.Sprintf("%d", stats.freeVWRChans)},
			[]string{"freeVWRChansCap", fmt.Sprintf("%d", stats.freeVWRChansCap)},
			[]string{"freeVWRChansIn", fmt.Sprintf("%d", stats.freeVWRChansIn)},
			[]string{"pendingVWRChans", fmt.Sprintf("%d", stats.pendingVWRChans)},
			[]string{"pendingVWRChansCap", fmt.Sprintf("%d", stats.pendingVWRChansCap)},
			[]string{"pendingVWRChansIn", fmt.Sprintf("%d", stats.pendingVWRChansIn)},
			[]string{"vfVMChanCap", fmt.Sprintf("%d", stats.vfVMChanCap)},
			[]string{"vfVMChanIn", fmt.Sprintf("%d", stats.vfVMChanIn)},
			[]string{"freeTOCBlockChanCap", fmt.Sprintf("%d", stats.freeTOCBlockChanCap)},
			[]string{"freeTOCBlockChanIn", fmt.Sprintf("%d", stats.freeTOCBlockChanIn)},
			[]string{"pendingTOCBlockChanCap", fmt.Sprintf("%d", stats.pendingTOCBlockChanCap)},
			[]string{"pendingTOCBlockChanIn", fmt.Sprintf("%d", stats.pendingTOCBlockChanIn)},
			[]string{"maxValueLocBlockID", fmt.Sprintf("%d", stats.maxValueLocBlockID)},
			[]string{"path", stats.path},
			[]string{"pathtoc", stats.pathtoc},
			[]string{"workers", fmt.Sprintf("%d", stats.workers)},
			[]string{"tombstoneDiscardInterval", fmt.Sprintf("%d", stats.tombstoneDiscardInterval)},
			[]string{"outPullReplicationWorkers", fmt.Sprintf("%d", stats.outPullReplicationWorkers)},
			[]string{"outPullReplicationInterval", fmt.Sprintf("%d", stats.outPullReplicationInterval)},
			[]string{"outPushReplicationWorkers", fmt.Sprintf("%d", stats.outPushReplicationWorkers)},
			[]string{"outPushReplicationInterval", fmt.Sprintf("%d", stats.outPushReplicationInterval)},
			[]string{"maxValueSize", fmt.Sprintf("%d", stats.maxValueSize)},
			[]string{"pageSize", fmt.Sprintf("%d", stats.pageSize)},
			[]string{"minValueAlloc", fmt.Sprintf("%d", stats.minValueAlloc)},
			[]string{"writePagesPerWorker", fmt.Sprintf("%d", stats.writePagesPerWorker)},
			[]string{"tombstoneAge", fmt.Sprintf("%d", stats.tombstoneAge)},
			[]string{"valuesFileSize", fmt.Sprintf("%d", stats.valuesFileSize)},
			[]string{"valuesFileReaders", fmt.Sprintf("%d", stats.valuesFileReaders)},
			[]string{"checksumInterval", fmt.Sprintf("%d", stats.checksumInterval)},
			[]string{"replicationIgnoreRecent", fmt.Sprintf("%d", stats.replicationIgnoreRecent)},
			[]string{"vlmCount", fmt.Sprintf("%d", stats.vlmCount)},
			[]string{"vlmLength", fmt.Sprintf("%d", stats.vlmLength)},
			[]string{"vlmDebugInfo", stats.vlmDebugInfo.String()},
		}, nil)
	} else {
		return brimtext.Align([][]string{
			[]string{"vlmCount", fmt.Sprintf("%d", stats.vlmCount)},
			[]string{"vlmLength", fmt.Sprintf("%d", stats.vlmLength)},
		}, nil)
	}
}
