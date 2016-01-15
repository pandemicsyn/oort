package store

import (
	"encoding/binary"
	"io"
	"sync"
	"sync/atomic"
)

// bsam: entries:n
// bsam entry: keyA:8, keyB:8, timestampbits:8

const _VALUE_BULK_SET_ACK_MSG_TYPE = 0x39589f4746844e3b
const _VALUE_BULK_SET_ACK_MSG_ENTRY_LENGTH = 24

type valueBulkSetAckState struct {
	inWorkers      int
	inMsgChan      chan *valueBulkSetAckMsg
	inFreeMsgChan  chan *valueBulkSetAckMsg
	outFreeMsgChan chan *valueBulkSetAckMsg

	inNotifyChanLock sync.Mutex
	inNotifyChan     chan *bgNotification
}

type valueBulkSetAckMsg struct {
	store *DefaultValueStore
	body  []byte
}

func (store *DefaultValueStore) bulkSetAckConfig(cfg *ValueStoreConfig) {
	store.bulkSetAckState.inWorkers = cfg.InBulkSetAckWorkers
	store.bulkSetAckState.inMsgChan = make(chan *valueBulkSetAckMsg, cfg.InBulkSetAckMsgs)
	store.bulkSetAckState.inFreeMsgChan = make(chan *valueBulkSetAckMsg, cfg.InBulkSetAckMsgs)
	for i := 0; i < cap(store.bulkSetAckState.inFreeMsgChan); i++ {
		store.bulkSetAckState.inFreeMsgChan <- &valueBulkSetAckMsg{
			store: store,
			body:  make([]byte, cfg.BulkSetAckMsgCap),
		}
	}
	store.bulkSetAckState.outFreeMsgChan = make(chan *valueBulkSetAckMsg, cfg.OutBulkSetAckMsgs)
	for i := 0; i < cap(store.bulkSetAckState.outFreeMsgChan); i++ {
		store.bulkSetAckState.outFreeMsgChan <- &valueBulkSetAckMsg{
			store: store,
			body:  make([]byte, cfg.BulkSetAckMsgCap),
		}
	}
	if store.msgRing != nil {
		store.msgRing.SetMsgHandler(_VALUE_BULK_SET_ACK_MSG_TYPE, store.newInBulkSetAckMsg)
	}
}

// EnableInBulkSetAck will resume handling incoming bulk set ack messages.
func (store *DefaultValueStore) EnableInBulkSetAck() {
	store.bulkSetAckState.inNotifyChanLock.Lock()
	if store.bulkSetAckState.inNotifyChan == nil {
		store.bulkSetAckState.inNotifyChan = make(chan *bgNotification, 1)
		go store.inBulkSetAckLauncher(store.bulkSetAckState.inNotifyChan)
	}
	store.bulkSetAckState.inNotifyChanLock.Unlock()
}

// DisableInBulkSetAck will stop handling any incoming bulk set ack messages
// (they will be dropped) until EnableInBulkSetAck is called.
func (store *DefaultValueStore) DisableInBulkSetAck() {
	store.bulkSetAckState.inNotifyChanLock.Lock()
	if store.bulkSetAckState.inNotifyChan != nil {
		c := make(chan struct{}, 1)
		store.bulkSetAckState.inNotifyChan <- &bgNotification{
			action:   _BG_DISABLE,
			doneChan: c,
		}
		<-c
		store.bulkSetAckState.inNotifyChan = nil
	}
	store.bulkSetAckState.inNotifyChanLock.Unlock()
}

func (store *DefaultValueStore) inBulkSetAckLauncher(notifyChan chan *bgNotification) {
	wg := &sync.WaitGroup{}
	wg.Add(store.bulkSetAckState.inWorkers)
	for i := 0; i < store.bulkSetAckState.inWorkers; i++ {
		go store.inBulkSetAck(wg)
	}
	var notification *bgNotification
	running := true
	for running {
		notification = <-notifyChan
		if notification.action == _BG_DISABLE {
			for i := 0; i < store.bulkSetAckState.inWorkers; i++ {
				store.bulkSetAckState.inMsgChan <- nil
			}
			wg.Wait()
			running = false
		} else {
			store.logCritical("inBulkSetAck: invalid action requested: %d", notification.action)
		}
		notification.doneChan <- struct{}{}
	}
}

// newInBulkSetAckMsg reads bulk-set-ack messages from the MsgRing and puts
// them on the inMsgChan for the inBulkSetAck workers to work on.
func (store *DefaultValueStore) newInBulkSetAckMsg(r io.Reader, l uint64) (uint64, error) {
	var bsam *valueBulkSetAckMsg
	select {
	case bsam = <-store.bulkSetAckState.inFreeMsgChan:
	default:
		// If there isn't a free valueBulkSetAckMsg, just read and discard the
		// incoming bulk-set-ack message.
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
				atomic.AddInt32(&store.inBulkSetAckInvalids, 1)
				return l - left, err
			}
		}
		atomic.AddInt32(&store.inBulkSetAckDrops, 1)
		return l, nil
	}
	var n int
	var sn int
	var err error
	// TODO: Need to read up the actual msg cap and toss rest.
	if l > uint64(cap(bsam.body)) {
		bsam.body = make([]byte, l)
	}
	bsam.body = bsam.body[:l]
	n = 0
	for n != len(bsam.body) {
		sn, err = r.Read(bsam.body[n:])
		n += sn
		if err != nil {
			store.bulkSetAckState.inFreeMsgChan <- bsam
			atomic.AddInt32(&store.inBulkSetAckInvalids, 1)
			return uint64(n), err
		}
	}
	store.bulkSetAckState.inMsgChan <- bsam
	atomic.AddInt32(&store.inBulkSetAcks, 1)
	return l, nil
}

// inBulkSetAck actually processes incoming bulk-set-ack messages; there may be
// more than one of these workers.
func (store *DefaultValueStore) inBulkSetAck(wg *sync.WaitGroup) {
	for {
		bsam := <-store.bulkSetAckState.inMsgChan
		if bsam == nil {
			break
		}
		ring := store.msgRing.Ring()
		var rightwardPartitionShift uint64
		if ring != nil {
			rightwardPartitionShift = 64 - uint64(ring.PartitionBitCount())
		}
		b := bsam.body
		// div mul just ensures any trailing bytes are dropped
		l := len(b) / _VALUE_BULK_SET_ACK_MSG_ENTRY_LENGTH * _VALUE_BULK_SET_ACK_MSG_ENTRY_LENGTH
		for o := 0; o < l; o += _VALUE_BULK_SET_ACK_MSG_ENTRY_LENGTH {
			keyA := binary.BigEndian.Uint64(b[o:])
			if ring != nil && !ring.Responsible(uint32(keyA>>rightwardPartitionShift)) {
				atomic.AddInt32(&store.inBulkSetAckWrites, 1)
				timestampbits := binary.BigEndian.Uint64(b[o+16:]) | _TSB_LOCAL_REMOVAL
				ptimestampbits, err := store.write(keyA, binary.BigEndian.Uint64(b[o+8:]), timestampbits, nil, true)
				if err != nil {
					atomic.AddInt32(&store.inBulkSetAckWriteErrors, 1)
				} else if ptimestampbits >= timestampbits {
					atomic.AddInt32(&store.inBulkSetAckWritesOverridden, 1)
				}
			}
		}
		store.bulkSetAckState.inFreeMsgChan <- bsam
	}
	wg.Done()
}

// newOutBulkSetAckMsg gives an initialized valueBulkSetAckMsg for filling out
// and eventually sending using the MsgRing. The MsgRing (or someone else if
// the message doesn't end up with the MsgRing) will call
// valueBulkSetAckMsg.Free() eventually and the valueBulkSetAckMsg will be
// requeued for reuse later. There is a fixed number of outgoing
// valueBulkSetAckMsg instances that can exist at any given time, capping
// memory usage. Once the limit is reached, this method will block until a
// valueBulkSetAckMsg is available to return.
func (store *DefaultValueStore) newOutBulkSetAckMsg() *valueBulkSetAckMsg {
	bsam := <-store.bulkSetAckState.outFreeMsgChan
	bsam.body = bsam.body[:0]
	return bsam
}

func (bsam *valueBulkSetAckMsg) MsgType() uint64 {
	return _VALUE_BULK_SET_ACK_MSG_TYPE
}

func (bsam *valueBulkSetAckMsg) MsgLength() uint64 {
	return uint64(len(bsam.body))
}

func (bsam *valueBulkSetAckMsg) WriteContent(w io.Writer) (uint64, error) {
	n, err := w.Write(bsam.body)
	return uint64(n), err
}

func (bsam *valueBulkSetAckMsg) Free() {
	bsam.store.bulkSetAckState.outFreeMsgChan <- bsam
}

func (bsam *valueBulkSetAckMsg) add(keyA uint64, keyB uint64, timestampbits uint64) bool {
	o := len(bsam.body)
	if o+_VALUE_BULK_SET_ACK_MSG_ENTRY_LENGTH >= cap(bsam.body) {
		return false
	}
	bsam.body = bsam.body[:o+_VALUE_BULK_SET_ACK_MSG_ENTRY_LENGTH]

	binary.BigEndian.PutUint64(bsam.body[o:], keyA)
	binary.BigEndian.PutUint64(bsam.body[o+8:], keyB)
	binary.BigEndian.PutUint64(bsam.body[o+16:], timestampbits)

	return true
}
