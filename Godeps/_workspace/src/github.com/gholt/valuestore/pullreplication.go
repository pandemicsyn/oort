package valuestore

import (
	"encoding/binary"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/gholt/brimtime.v1"
)

const _GLH_IN_PULL_REPLICATION_MSGS = 128
const _GLH_IN_PULL_REPLICATION_HANDLERS = 40
const _GLH_IN_PULL_REPLICATION_MSG_TIMEOUT = 300
const _GLH_OUT_PULL_REPLICATION_MSGS = 128
const _GLH_BLOOM_FILTER_N = 1000000
const _GLH_BLOOM_FILTER_P = 0.001
const _MSG_PULL_REPLICATION = 0x579c4bd162f045b3
const pullReplicationMsgHeaderBytes = 44

type pullReplicationState struct {
	inMsgChan     chan *pullReplicationMsg
	inFreeMsgChan chan *pullReplicationMsg
	outWorkers    uint64
	outInterval   int
	outNotifyChan chan *backgroundNotification
	outIteration  uint16
	outAbort      uint32
	outMsgChan    chan *pullReplicationMsg
	outKTBFs      []*ktBloomFilter
}

type pullReplicationMsg struct {
	vs     *DefaultValueStore
	header []byte
	body   []byte
}

func (vs *DefaultValueStore) pullReplicationInit(cfg *config) {
	vs.pullReplicationState.outInterval = cfg.outPullReplicationInterval
	vs.pullReplicationState.outNotifyChan = make(chan *backgroundNotification, 1)
	vs.pullReplicationState.outWorkers = uint64(cfg.outPullReplicationWorkers)
	vs.pullReplicationState.outIteration = uint16(cfg.rand.Uint32())
	if vs.msgRing != nil {
		vs.msgRing.SetMsgHandler(_MSG_PULL_REPLICATION, vs.newInPullReplicationMsg)
		vs.pullReplicationState.inMsgChan = make(chan *pullReplicationMsg, _GLH_IN_PULL_REPLICATION_MSGS)
		vs.pullReplicationState.inFreeMsgChan = make(chan *pullReplicationMsg, _GLH_IN_PULL_REPLICATION_MSGS)
		for i := 0; i < cap(vs.pullReplicationState.inFreeMsgChan); i++ {
			vs.pullReplicationState.inFreeMsgChan <- &pullReplicationMsg{
				vs:     vs,
				header: make([]byte, ktBloomFilterHeaderBytes+pullReplicationMsgHeaderBytes),
			}
		}
		for i := 0; i < _GLH_IN_PULL_REPLICATION_HANDLERS; i++ {
			go vs.inPullReplication()
		}
		vs.pullReplicationState.outMsgChan = make(chan *pullReplicationMsg, _GLH_OUT_PULL_REPLICATION_MSGS)
		vs.pullReplicationState.outKTBFs = []*ktBloomFilter{newKTBloomFilter(_GLH_BLOOM_FILTER_N, _GLH_BLOOM_FILTER_P, 0)}
		for i := 0; i < cap(vs.pullReplicationState.outMsgChan); i++ {
			vs.pullReplicationState.outMsgChan <- &pullReplicationMsg{
				vs:     vs,
				header: make([]byte, ktBloomFilterHeaderBytes+pullReplicationMsgHeaderBytes),
				body:   make([]byte, len(vs.pullReplicationState.outKTBFs[0].bits)),
			}
		}
	}
	vs.pullReplicationState.outNotifyChan = make(chan *backgroundNotification, 1)
	go vs.outPullReplicationLauncher()
}

// DisableOutPullReplication will stop any outgoing pull replication requests
// until EnableOutPullReplication is called.
func (vs *DefaultValueStore) DisableOutPullReplication() {
	c := make(chan struct{}, 1)
	vs.pullReplicationState.outNotifyChan <- &backgroundNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableOutPullReplication will resume outgoing pull replication requests.
func (vs *DefaultValueStore) EnableOutPullReplication() {
	c := make(chan struct{}, 1)
	vs.pullReplicationState.outNotifyChan <- &backgroundNotification{
		enable:   true,
		doneChan: c,
	}
	<-c
}

func (vs *DefaultValueStore) inPullReplication() {
	// Max keys; 28 = keyA:8, keyB:8, timestampbits:8, length:4, value:0
	k := make([]uint64, _GLH_OUT_BULK_SET_MSG_SIZE/28)
	v := make([]byte, vs.maxValueSize)
	for {
		prm := <-vs.pullReplicationState.inMsgChan
		k = k[:0]
		cutoff := prm.cutoff()
		tombstoneCutoff := (uint64(brimtime.TimeToUnixMicro(time.Now())) << _TSB_UTIL_BITS) - vs.tombstoneDiscardState.age
		ktbf := prm.ktBloomFilter()
		l := int64(_GLH_OUT_BULK_SET_MSG_SIZE)
		vs.vlm.ScanCallback(prm.rangeStart(), prm.rangeStop(), 0, _TSB_LOCAL_REMOVAL, cutoff, uint64(len(k)), func(keyA uint64, keyB uint64, timestampbits uint64, length uint32) {
			if l > 0 {
				if timestampbits&_TSB_DELETION == 0 || timestampbits >= tombstoneCutoff {
					if !ktbf.mayHave(keyA, keyB, timestampbits) {
						k = append(k, keyA, keyB)
						// bsm: keyA:8, keyB:8, timestampbits:8, length:4,
						//      value:n
						l -= 28 + int64(length)
					}
				}
			}
		})
		nodeID := prm.nodeID()
		vs.pullReplicationState.inFreeMsgChan <- prm
		if len(k) > 0 {
			bsm := vs.newOutBulkSetMsg()
			var t uint64
			var err error
			for i := 0; i < len(k); i += 2 {
				t, v, err = vs.read(k[i], k[i+1], v[:0])
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
				}
			}
			if len(bsm.body) > 0 {
				vs.msgRing.MsgToNode(nodeID, bsm)
			}
		}
	}
}

// OutPullReplicationPass will immediately execute an outgoing pull replication
// pass rather than waiting for the next interval. If a pass is currently
// executing, it will be stopped and restarted so that a call to this function
// ensures one complete pass occurs. Note that this pass will send the outgoing
// pull replication requests, but all the responses will almost certainly not
// have been received when this function returns. These requests are stateless,
// and so synchronization at that level is not possible.
func (vs *DefaultValueStore) OutPullReplicationPass() {
	atomic.StoreUint32(&vs.pullReplicationState.outAbort, 1)
	c := make(chan struct{}, 1)
	vs.pullReplicationState.outNotifyChan <- &backgroundNotification{doneChan: c}
	<-c
}

func (vs *DefaultValueStore) outPullReplicationLauncher() {
	var enabled bool
	interval := float64(vs.pullReplicationState.outInterval) * float64(time.Second)
	nextRun := time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
	for {
		var notification *backgroundNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-vs.pullReplicationState.outNotifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-vs.pullReplicationState.outNotifyChan:
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
				atomic.StoreUint32(&vs.pullReplicationState.outAbort, 1)
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&vs.pullReplicationState.outAbort, 0)
			vs.outPullReplicationPass()
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&vs.pullReplicationState.outAbort, 0)
			vs.outPullReplicationPass()
		}
	}
}

func (vs *DefaultValueStore) outPullReplicationPass() {
	if vs.msgRing == nil {
		return
	}
	if vs.logDebug != nil {
		begin := time.Now()
		defer func() {
			vs.logDebug.Printf("out pull replication pass took %s", time.Now().Sub(begin))
		}()
	}
	ring := vs.msgRing.Ring()
	rightwardPartitionShift := 64 - ring.PartitionBitCount()
	partitionCount := uint64(1) << ring.PartitionBitCount()
	if vs.pullReplicationState.outIteration == math.MaxUint16 {
		vs.pullReplicationState.outIteration = 0
	} else {
		vs.pullReplicationState.outIteration++
	}
	ringVersion := ring.Version()
	ws := vs.pullReplicationState.outWorkers
	for uint64(len(vs.pullReplicationState.outKTBFs)) < ws {
		vs.pullReplicationState.outKTBFs = append(vs.pullReplicationState.outKTBFs, newKTBloomFilter(_GLH_BLOOM_FILTER_N, _GLH_BLOOM_FILTER_P, 0))
	}
	f := func(p uint64, w uint64, ktbf *ktBloomFilter) {
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
		cutoff := timestampbitsnow - vs.replicationIgnoreRecent
		var more bool
		for {
			rbThis := rb
			ktbf.reset(vs.pullReplicationState.outIteration)
			rb, more = vs.vlm.ScanCallback(rb, re, 0, _TSB_LOCAL_REMOVAL, cutoff, _GLH_BLOOM_FILTER_N, func(keyA uint64, keyB uint64, timestampbits uint64, length uint32) {
				ktbf.add(keyA, keyB, timestampbits)
			})
			if atomic.LoadUint32(&vs.pullReplicationState.outAbort) != 0 {
				break
			}
			if vs.msgRing.Ring().Version() != ringVersion {
				break
			}
			reThis := re
			if more {
				reThis = rb - 1
			}
			prm := vs.newOutPullReplicationMsg(ringVersion, uint32(p), cutoff, rbThis, reThis, ktbf)
			vs.msgRing.MsgToOtherReplicas(ringVersion, uint32(p), prm)
			if !more {
				break
			}
		}
	}
	wg := &sync.WaitGroup{}
	wg.Add(int(ws))
	for w := uint64(0); w < ws; w++ {
		go func(w uint64) {
			ktbf := vs.pullReplicationState.outKTBFs[w]
			pb := partitionCount / ws * w
			for p := pb; p < partitionCount; p++ {
				if atomic.LoadUint32(&vs.pullReplicationState.outAbort) != 0 {
					break
				}
				if ring.Version() != ringVersion {
					break
				}
				if ring.Responsible(uint32(p)) {
					f(p, w, ktbf)
				}
			}
			for p := uint64(0); p < pb; p++ {
				if atomic.LoadUint32(&vs.pullReplicationState.outAbort) != 0 {
					break
				}
				if ring.Version() != ringVersion {
					break
				}
				if ring.Responsible(uint32(p)) {
					f(p, w, ktbf)
				}
			}
			wg.Done()
		}(w)
	}
	wg.Wait()
}

var toss []byte = make([]byte, 65536)

func (vs *DefaultValueStore) newInPullReplicationMsg(r io.Reader, l uint64) (uint64, error) {
	var prm *pullReplicationMsg
	select {
	case prm = <-vs.pullReplicationState.inFreeMsgChan:
	case <-time.After(_GLH_IN_PULL_REPLICATION_MSG_TIMEOUT * time.Second):
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
				return l - left, err
			}
		}
		return l, nil
	}
	bl := l - pullReplicationMsgHeaderBytes - uint64(ktBloomFilterHeaderBytes)
	if uint64(cap(prm.body)) < bl {
		prm.body = make([]byte, bl)
	}
	prm.body = prm.body[:bl]
	var n int
	var sn int
	var err error
	for n != len(prm.header) {
		if err != nil {
			return uint64(n), err
		}
		sn, err = r.Read(prm.header[n:])
		n += sn
	}
	n = 0
	for n != len(prm.body) {
		if err != nil {
			return uint64(len(prm.header)) + uint64(n), err
		}
		sn, err = r.Read(prm.body[n:])
		n += sn
	}
	vs.pullReplicationState.inMsgChan <- prm
	return l, nil
}

func (vs *DefaultValueStore) newOutPullReplicationMsg(ringVersion int64, partition uint32, cutoff uint64, rangeStart uint64, rangeStop uint64, ktbf *ktBloomFilter) *pullReplicationMsg {
	prm := <-vs.pullReplicationState.outMsgChan
	binary.BigEndian.PutUint64(prm.header, vs.msgRing.Ring().LocalNode().ID())
	binary.BigEndian.PutUint64(prm.header[8:], uint64(ringVersion))
	binary.BigEndian.PutUint32(prm.header[16:], partition)
	binary.BigEndian.PutUint64(prm.header[20:], cutoff)
	binary.BigEndian.PutUint64(prm.header[28:], rangeStart)
	binary.BigEndian.PutUint64(prm.header[36:], rangeStop)
	ktbf.toMsg(prm, pullReplicationMsgHeaderBytes)
	return prm
}

func (prm *pullReplicationMsg) MsgType() uint64 {
	return _MSG_PULL_REPLICATION
}

func (prm *pullReplicationMsg) MsgLength() uint64 {
	return uint64(len(prm.header)) + uint64(len(prm.body))
}

func (prm *pullReplicationMsg) nodeID() uint64 {
	return binary.BigEndian.Uint64(prm.header)
}

func (prm *pullReplicationMsg) ringVersion() int64 {
	return int64(binary.BigEndian.Uint64(prm.header[8:]))
}

func (prm *pullReplicationMsg) partition() uint32 {
	return binary.BigEndian.Uint32(prm.header[16:])
}

func (prm *pullReplicationMsg) cutoff() uint64 {
	return binary.BigEndian.Uint64(prm.header[20:])
}

func (prm *pullReplicationMsg) rangeStart() uint64 {
	return binary.BigEndian.Uint64(prm.header[28:])
}

func (prm *pullReplicationMsg) rangeStop() uint64 {
	return binary.BigEndian.Uint64(prm.header[36:])
}

func (prm *pullReplicationMsg) ktBloomFilter() *ktBloomFilter {
	return newKTBloomFilterFromMsg(prm, pullReplicationMsgHeaderBytes)
}

func (prm *pullReplicationMsg) WriteContent(w io.Writer) (uint64, error) {
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

func (prm *pullReplicationMsg) Done() {
	prm.vs.pullReplicationState.outMsgChan <- prm
}
