package store

import (
	"encoding/binary"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/gholt/brimtime.v1"
)

const _VALUE_PULL_REPLICATION_MSG_TYPE = 0x579c4bd162f045b3

const _VALUE_PULL_REPLICATION_MSG_HEADER_BYTES = 44

type valuePullReplicationState struct {
	outInterval          int
	outWorkers           uint64
	outIteration         uint16
	inMsgChan            chan *valuePullReplicationMsg
	inFreeMsgChan        chan *valuePullReplicationMsg
	inWorkers            int
	outMsgChan           chan *valuePullReplicationMsg
	bloomN               uint64
	bloomP               float64
	outKTBFs             []*valueKTBloomFilter
	inResponseMsgTimeout time.Duration
	outMsgTimeout        time.Duration

	inNotifyChanLock  sync.Mutex
	inNotifyChan      chan *bgNotification
	outNotifyChanLock sync.Mutex
	outNotifyChan     chan *bgNotification
}

type valuePullReplicationMsg struct {
	store  *DefaultValueStore
	header []byte
	body   []byte
}

func (store *DefaultValueStore) pullReplicationConfig(cfg *ValueStoreConfig) {
	store.pullReplicationState.outInterval = cfg.OutPullReplicationInterval
	store.pullReplicationState.outWorkers = uint64(cfg.OutPullReplicationWorkers)
	store.pullReplicationState.outIteration = uint16(cfg.Rand.Uint32())
	if store.msgRing != nil {
		store.msgRing.SetMsgHandler(_VALUE_PULL_REPLICATION_MSG_TYPE, store.newInPullReplicationMsg)
		store.pullReplicationState.inMsgChan = make(chan *valuePullReplicationMsg, cfg.InPullReplicationMsgs)
		store.pullReplicationState.inFreeMsgChan = make(chan *valuePullReplicationMsg, cfg.InPullReplicationMsgs)
		for i := 0; i < cap(store.pullReplicationState.inFreeMsgChan); i++ {
			store.pullReplicationState.inFreeMsgChan <- &valuePullReplicationMsg{
				store:  store,
				header: make([]byte, _VALUE_KT_BLOOM_FILTER_HEADER_BYTES+_VALUE_PULL_REPLICATION_MSG_HEADER_BYTES),
			}
		}
		store.pullReplicationState.inWorkers = cfg.InPullReplicationWorkers
		store.pullReplicationState.outMsgChan = make(chan *valuePullReplicationMsg, cfg.OutPullReplicationMsgs)
		store.pullReplicationState.bloomN = uint64(cfg.OutPullReplicationBloomN)
		store.pullReplicationState.bloomP = cfg.OutPullReplicationBloomP
		store.pullReplicationState.outKTBFs = []*valueKTBloomFilter{newValueKTBloomFilter(store.pullReplicationState.bloomN, store.pullReplicationState.bloomP, 0)}
		for i := 0; i < cap(store.pullReplicationState.outMsgChan); i++ {
			store.pullReplicationState.outMsgChan <- &valuePullReplicationMsg{
				store:  store,
				header: make([]byte, _VALUE_KT_BLOOM_FILTER_HEADER_BYTES+_VALUE_PULL_REPLICATION_MSG_HEADER_BYTES),
				body:   make([]byte, len(store.pullReplicationState.outKTBFs[0].bits)),
			}
		}
		store.pullReplicationState.inResponseMsgTimeout = time.Duration(cfg.InPullReplicationResponseMsgTimeout) * time.Millisecond
		store.pullReplicationState.outMsgTimeout = time.Duration(cfg.OutPullReplicationMsgTimeout) * time.Millisecond
	}
}

// EnableInPullReplication will resume handling incoming pull replication
// requests.
func (store *DefaultValueStore) EnableInPullReplication() {
	store.pullReplicationState.inNotifyChanLock.Lock()
	if store.pullReplicationState.inNotifyChan == nil {
		store.pullReplicationState.inNotifyChan = make(chan *bgNotification, 1)
		go store.inPullReplicationLauncher(store.pullReplicationState.inNotifyChan)
	}
	store.pullReplicationState.inNotifyChanLock.Unlock()
}

// DisableInPullReplication will stop handling any incoming pull replication
// requests (they will be dropped) until EnableInPullReplication is called.
func (store *DefaultValueStore) DisableInPullReplication() {
	store.pullReplicationState.inNotifyChanLock.Lock()
	if store.pullReplicationState.inNotifyChan != nil {
		c := make(chan struct{}, 1)
		store.pullReplicationState.inNotifyChan <- &bgNotification{
			action:   _BG_DISABLE,
			doneChan: c,
		}
		<-c
		store.pullReplicationState.inNotifyChan = nil
	}
	store.pullReplicationState.inNotifyChanLock.Unlock()
}

// OutPullReplicationPass will immediately execute an outgoing pull replication
// pass rather than waiting for the next interval. If a pass is currently
// executing, it will be stopped and restarted so that a call to this function
// ensures one complete pass occurs. Note that this pass will send the outgoing
// pull replication requests, but all the responses will almost certainly not
// have been received when this function returns. These requests are stateless,
// and so synchronization at that level is not possible.
func (store *DefaultValueStore) OutPullReplicationPass() {
	store.pullReplicationState.outNotifyChanLock.Lock()
	if store.pullReplicationState.outNotifyChan == nil {
		store.outPullReplicationPass(make(chan *bgNotification))
	} else {
		c := make(chan struct{}, 1)
		store.pullReplicationState.outNotifyChan <- &bgNotification{
			action:   _BG_PASS,
			doneChan: c,
		}
		<-c
	}
	store.pullReplicationState.outNotifyChanLock.Unlock()
}

// EnableOutPullReplication will resume outgoing pull replication requests.
func (store *DefaultValueStore) EnableOutPullReplication() {
	store.pullReplicationState.outNotifyChanLock.Lock()
	if store.pullReplicationState.outNotifyChan == nil {
		store.pullReplicationState.outNotifyChan = make(chan *bgNotification, 1)
		go store.outPullReplicationLauncher(store.pullReplicationState.outNotifyChan)
	}
	store.pullReplicationState.outNotifyChanLock.Unlock()
}

// DisableOutPullReplication will stop any outgoing pull replication requests
// until EnableOutPullReplication is called.
func (store *DefaultValueStore) DisableOutPullReplication() {
	store.pullReplicationState.outNotifyChanLock.Lock()
	if store.pullReplicationState.outNotifyChan != nil {
		c := make(chan struct{}, 1)
		store.pullReplicationState.outNotifyChan <- &bgNotification{
			action:   _BG_DISABLE,
			doneChan: c,
		}
		<-c
		store.pullReplicationState.outNotifyChan = nil
	}
	store.pullReplicationState.outNotifyChanLock.Unlock()
}

func (store *DefaultValueStore) inPullReplicationLauncher(notifyChan chan *bgNotification) {
	wg := &sync.WaitGroup{}
	wg.Add(store.pullReplicationState.inWorkers)
	for i := 0; i < store.pullReplicationState.inWorkers; i++ {
		go store.inPullReplication(wg)
	}
	var notification *bgNotification
	running := true
	for running {
		notification = <-notifyChan
		if notification.action == _BG_DISABLE {
			for i := 0; i < store.pullReplicationState.inWorkers; i++ {
				store.pullReplicationState.inMsgChan <- nil
			}
			wg.Wait()
			running = false
		} else {
			store.logCritical("outPullReplication: invalid action requested: %d", notification.action)
		}
		notification.doneChan <- struct{}{}
	}
}

// newInPullReplicationMsg reads pull-replication messages from the MsgRing and
// puts them on the inMsgChan for the inPullReplication workers to work on.
func (store *DefaultValueStore) newInPullReplicationMsg(r io.Reader, l uint64) (uint64, error) {
	var prm *valuePullReplicationMsg
	select {
	case prm = <-store.pullReplicationState.inFreeMsgChan:
	default:
		// If there isn't a free valuePullReplicationMsg, just read and
		// discard the incoming pull-replication message.
		left := l
		var sn int
		var err error
		for left > 0 {
			t := toss
			if left < uint64(len(t)) {
				t = t[:left]
			}
			sn, err = r.Read(t)
			left -= uint64(sn)
			if err != nil {
				atomic.AddInt32(&store.inPullReplicationInvalids, 1)
				return l - left, err
			}
		}
		atomic.AddInt32(&store.inPullReplicationDrops, 1)
		return l, nil
	}
	// TODO: We need to cap this so memory isn't abused in case someone
	// accidentally sets a crazy sized bloom filter on another node. Since a
	// partial pull-replication message is pretty much useless as it would drop
	// a chunk of the bloom filter bitspace, we should drop oversized messages
	// but report the issue.
	bl := l - _VALUE_PULL_REPLICATION_MSG_HEADER_BYTES - uint64(_VALUE_KT_BLOOM_FILTER_HEADER_BYTES)
	if uint64(cap(prm.body)) < bl {
		prm.body = make([]byte, bl)
	}
	prm.body = prm.body[:bl]
	var n int
	var sn int
	var err error
	for n != len(prm.header) {
		if err != nil {
			store.pullReplicationState.inFreeMsgChan <- prm
			atomic.AddInt32(&store.inPullReplicationInvalids, 1)
			return uint64(n), err
		}
		sn, err = r.Read(prm.header[n:])
		n += sn
	}
	n = 0
	for n != len(prm.body) {
		if err != nil {
			store.pullReplicationState.inFreeMsgChan <- prm
			atomic.AddInt32(&store.inPullReplicationInvalids, 1)
			return uint64(len(prm.header)) + uint64(n), err
		}
		sn, err = r.Read(prm.body[n:])
		n += sn
	}
	store.pullReplicationState.inMsgChan <- prm
	atomic.AddInt32(&store.inPullReplications, 1)
	return l, nil
}

// inPullReplication actually processes incoming pull-replication messages;
// there may be more than one of these workers.
func (store *DefaultValueStore) inPullReplication(wg *sync.WaitGroup) {
	k := make([]uint64, store.bulkSetState.msgCap/_VALUE_BULK_SET_MSG_MIN_ENTRY_LENGTH*2)
	v := make([]byte, store.valueCap)
	for {
		prm := <-store.pullReplicationState.inMsgChan
		if prm == nil {
			break
		}
		ring := store.msgRing.Ring()
		if ring == nil {
			store.pullReplicationState.inFreeMsgChan <- prm
			continue
		}
		k = k[:0]
		// This is what the remote system used when making its bloom filter,
		// computed via its config.ReplicationIgnoreRecent setting. We want to
		// use the exact same cutoff in our checks and possible response.
		cutoff := prm.cutoff()
		tombstoneCutoff := (uint64(brimtime.TimeToUnixMicro(time.Now())) << _TSB_UTIL_BITS) - store.tombstoneDiscardState.age
		ktbf := prm.ktBloomFilter()
		l := int64(store.bulkSetState.msgCap)
		callback := func(keyA uint64, keyB uint64, timestampbits uint64, length uint32) bool {
			if timestampbits&_TSB_DELETION == 0 || timestampbits >= tombstoneCutoff {
				if !ktbf.mayHave(keyA, keyB, timestampbits) {
					k = append(k, keyA, keyB)
					l -= _VALUE_BULK_SET_MSG_ENTRY_HEADER_LENGTH + int64(length)
					if l <= 0 {
						return false
					}
				}
			}
			return true
		}
		// Based on the replica index for the local node, start the scan at
		// different points. For example, in a three replica system the first
		// replica would start scanning at the start, the second a third
		// through, the last would start two thirds through. This is so that
		// pull-replication messages, which are sent concurrently to all other
		// replicas, will get different responses back instead of duplicate
		// items if there is a lot of data to be sent.
		scanStart := prm.rangeStart() + (prm.rangeStop()-prm.rangeStart())/uint64(ring.ReplicaCount())*uint64(ring.ResponsibleReplica(uint32(prm.rangeStart()>>(64-ring.PartitionBitCount()))))
		scanStop := prm.rangeStop()
		store.locmap.ScanCallback(scanStart, scanStop, 0, _TSB_LOCAL_REMOVAL, cutoff, math.MaxUint64, callback)
		if l > 0 {
			scanStop = scanStart - 1
			scanStart = prm.rangeStart()
			store.locmap.ScanCallback(scanStart, scanStop, 0, _TSB_LOCAL_REMOVAL, cutoff, math.MaxUint64, callback)
		}
		nodeID := prm.nodeID()
		store.pullReplicationState.inFreeMsgChan <- prm
		if len(k) > 0 {
			bsm := store.newOutBulkSetMsg()
			// Indicate that a response to this bulk-set message is not
			// necessary. If the message fails to reach its destination, that
			// destination will simply resend another pull replication message
			// on its next pass.
			binary.BigEndian.PutUint64(bsm.header, 0)
			var t uint64
			var err error
			for i := 0; i < len(k); i += 2 {
				t, v, err = store.read(k[i], k[i+1], v[:0])
				if err == ErrNotFound {
					if t == 0 {
						continue
					}
				} else if err != nil {
					continue
				}
				if t&_TSB_LOCAL_REMOVAL == 0 {
					if !bsm.add(k[i], k[i+1], t, v) {
						break
					}
					atomic.AddInt32(&store.outBulkSetValues, 1)
				}
			}
			if len(bsm.body) > 0 {
				atomic.AddInt32(&store.outBulkSets, 1)
				store.msgRing.MsgToNode(bsm, nodeID, store.pullReplicationState.inResponseMsgTimeout)
			}
		}
	}
	wg.Done()
}

func (store *DefaultValueStore) outPullReplicationLauncher(notifyChan chan *bgNotification) {
	interval := float64(store.pullReplicationState.outInterval) * float64(time.Second)
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
				nextNotification = store.outPullReplicationPass(notifyChan)
			case _BG_DISABLE:
				running = false
			default:
				store.logCritical("outPullReplication: invalid action requested: %d", notification.action)
			}
			notification.doneChan <- struct{}{}
			notification = nextNotification
		} else {
			notification = store.outPullReplicationPass(notifyChan)
		}
	}
}

func (store *DefaultValueStore) outPullReplicationPass(notifyChan chan *bgNotification) *bgNotification {
	if store.msgRing == nil {
		return nil
	}
	if store.logDebug != nil {
		begin := time.Now()
		defer func() {
			store.logDebug("outPullReplication: pass took %s", time.Now().Sub(begin))
		}()
	}
	ring := store.msgRing.Ring()
	if ring == nil {
		return nil
	}
	rightwardPartitionShift := 64 - ring.PartitionBitCount()
	partitionCount := uint64(1) << ring.PartitionBitCount()
	if store.pullReplicationState.outIteration == math.MaxUint16 {
		store.pullReplicationState.outIteration = 0
	} else {
		store.pullReplicationState.outIteration++
	}
	ringVersion := ring.Version()
	ws := store.pullReplicationState.outWorkers
	for uint64(len(store.pullReplicationState.outKTBFs)) < ws {
		store.pullReplicationState.outKTBFs = append(store.pullReplicationState.outKTBFs, newValueKTBloomFilter(store.pullReplicationState.bloomN, store.pullReplicationState.bloomP, 0))
	}
	var abort uint32
	f := func(p uint64, w uint64, ktbf *valueKTBloomFilter) {
		pb := p << rightwardPartitionShift
		rb := pb + ((uint64(1) << rightwardPartitionShift) / ws * w)
		var re uint64
		if w+1 == ws {
			if p+1 == partitionCount {
				re = math.MaxUint64
			} else {
				re = ((p + 1) << rightwardPartitionShift) - 1
			}
		} else {
			re = pb + ((uint64(1) << rightwardPartitionShift) / ws * (w + 1)) - 1
		}
		timestampbitsnow := uint64(brimtime.TimeToUnixMicro(time.Now())) << _TSB_UTIL_BITS
		cutoff := timestampbitsnow - store.replicationIgnoreRecent
		var more bool
		for atomic.LoadUint32(&abort) == 0 {
			rbThis := rb
			ktbf.reset(store.pullReplicationState.outIteration)
			rb, more = store.locmap.ScanCallback(rb, re, 0, _TSB_LOCAL_REMOVAL, cutoff, store.pullReplicationState.bloomN, func(keyA uint64, keyB uint64, timestampbits uint64, length uint32) bool {
				ktbf.add(keyA, keyB, timestampbits)
				return true
			})
			ring2 := store.msgRing.Ring()
			if ring2 == nil || ring2.Version() != ringVersion {
				break
			}
			reThis := re
			if more {
				reThis = rb - 1
			}
			prm := store.newOutPullReplicationMsg(ringVersion, uint32(p), cutoff, rbThis, reThis, ktbf)
			atomic.AddInt32(&store.outPullReplications, 1)
			store.msgRing.MsgToOtherReplicas(prm, uint32(p), store.pullReplicationState.outMsgTimeout)
			if !more {
				break
			}
		}
	}
	wg := &sync.WaitGroup{}
	wg.Add(int(ws))
	for w := uint64(0); w < ws; w++ {
		go func(w uint64) {
			ktbf := store.pullReplicationState.outKTBFs[w]
			pb := partitionCount / ws * w
			for p := pb; p < partitionCount; p++ {
				if atomic.LoadUint32(&abort) != 0 {
					break
				}
				ring2 := store.msgRing.Ring()
				if ring2 == nil || ring2.Version() != ringVersion {
					break
				}
				if ring.Responsible(uint32(p)) {
					f(p, w, ktbf)
				}
			}
			for p := uint64(0); p < pb; p++ {
				if atomic.LoadUint32(&abort) != 0 {
					break
				}
				ring2 := store.msgRing.Ring()
				if ring2 == nil || ring2.Version() != ringVersion {
					break
				}
				if ring.Responsible(uint32(p)) {
					f(p, w, ktbf)
				}
			}
			wg.Done()
		}(w)
	}
	waitChan := make(chan struct{}, 1)
	go func() {
		wg.Wait()
		close(waitChan)
	}()
	select {
	case notification := <-notifyChan:
		atomic.AddUint32(&abort, 1)
		<-waitChan
		return notification
	case <-waitChan:
		return nil
	}
}

// newOutPullReplicationMsg gives an initialized valuePullReplicationMsg for filling
// out and eventually sending using the MsgRing. The MsgRing (or someone else
// if the message doesn't end up with the MsgRing) will call
// valuePullReplicationMsg.Free() eventually and the pullReplicationMsg will be
// requeued for reuse later. There is a fixed number of outgoing
// valuePullReplicationMsg instances that can exist at any given time, capping
// memory usage. Once the limit is reached, this method will block until a
// valuePullReplicationMsg is available to return.
func (store *DefaultValueStore) newOutPullReplicationMsg(ringVersion int64, partition uint32, cutoff uint64, rangeStart uint64, rangeStop uint64, ktbf *valueKTBloomFilter) *valuePullReplicationMsg {
	prm := <-store.pullReplicationState.outMsgChan
	if store.msgRing != nil {
		if r := store.msgRing.Ring(); r != nil {
			if n := r.LocalNode(); n != nil {
				binary.BigEndian.PutUint64(prm.header, n.ID())
			}
		}
	}
	binary.BigEndian.PutUint64(prm.header[8:], uint64(ringVersion))
	binary.BigEndian.PutUint32(prm.header[16:], partition)
	binary.BigEndian.PutUint64(prm.header[20:], cutoff)
	binary.BigEndian.PutUint64(prm.header[28:], rangeStart)
	binary.BigEndian.PutUint64(prm.header[36:], rangeStop)
	ktbf.toMsg(prm, _VALUE_PULL_REPLICATION_MSG_HEADER_BYTES)
	return prm
}

func (prm *valuePullReplicationMsg) MsgType() uint64 {
	return _VALUE_PULL_REPLICATION_MSG_TYPE
}

func (prm *valuePullReplicationMsg) MsgLength() uint64 {
	return uint64(len(prm.header)) + uint64(len(prm.body))
}

func (prm *valuePullReplicationMsg) nodeID() uint64 {
	return binary.BigEndian.Uint64(prm.header)
}

func (prm *valuePullReplicationMsg) ringVersion() int64 {
	return int64(binary.BigEndian.Uint64(prm.header[8:]))
}

func (prm *valuePullReplicationMsg) partition() uint32 {
	return binary.BigEndian.Uint32(prm.header[16:])
}

func (prm *valuePullReplicationMsg) cutoff() uint64 {
	return binary.BigEndian.Uint64(prm.header[20:])
}

func (prm *valuePullReplicationMsg) rangeStart() uint64 {
	return binary.BigEndian.Uint64(prm.header[28:])
}

func (prm *valuePullReplicationMsg) rangeStop() uint64 {
	return binary.BigEndian.Uint64(prm.header[36:])
}

func (prm *valuePullReplicationMsg) ktBloomFilter() *valueKTBloomFilter {
	return newValueKTBloomFilterFromMsg(prm, _VALUE_PULL_REPLICATION_MSG_HEADER_BYTES)
}

func (prm *valuePullReplicationMsg) WriteContent(w io.Writer) (uint64, error) {
	var n int
	var sn int
	var err error
	sn, err = w.Write(prm.header)
	n += sn
	if err != nil {
		return uint64(n), err
	}
	sn, err = w.Write(prm.body)
	n += sn
	return uint64(n), err
}

func (prm *valuePullReplicationMsg) Free() {
	prm.store.pullReplicationState.outMsgChan <- prm
}
