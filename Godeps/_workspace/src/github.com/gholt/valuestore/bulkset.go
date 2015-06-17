package valuestore

import (
	"encoding/binary"
	"io"
	"time"
)

const _GLH_IN_BULK_SET_MSGS = 128
const _GLH_IN_BULK_SET_HANDLERS = 40
const _GLH_OUT_BULK_SET_MSGS = 128
const _GLH_OUT_BULK_SET_MSG_SIZE = 16 * 1024 * 1024
const _GLH_IN_BULK_SET_MSG_TIMEOUT = 300
const _MSG_BULK_SET = 0x44f58445991a4aa1

type bulkSetState struct {
	inMsgChan     chan *bulkSetMsg
	inFreeMsgChan chan *bulkSetMsg
	outMsgChan    chan *bulkSetMsg
}

type bulkSetMsg struct {
	vs     *DefaultValueStore
	header []byte
	body   []byte
}

func (vs *DefaultValueStore) bulkSetInit(cfg *config) {
	if vs.msgRing != nil {
		vs.msgRing.SetMsgHandler(_MSG_BULK_SET, vs.newInBulkSetMsg)
		vs.bulkSetState.inMsgChan = make(chan *bulkSetMsg, _GLH_IN_BULK_SET_MSGS)
		vs.bulkSetState.inFreeMsgChan = make(chan *bulkSetMsg, _GLH_IN_BULK_SET_MSGS)
		for i := 0; i < cap(vs.bulkSetState.inFreeMsgChan); i++ {
			vs.bulkSetState.inFreeMsgChan <- &bulkSetMsg{
				vs:     vs,
				header: make([]byte, 8),
			}
		}
		for i := 0; i < _GLH_IN_BULK_SET_HANDLERS; i++ {
			go vs.inBulkSet()
		}
		vs.bulkSetState.outMsgChan = make(chan *bulkSetMsg, _GLH_OUT_BULK_SET_MSGS)
		for i := 0; i < cap(vs.bulkSetState.outMsgChan); i++ {
			vs.bulkSetState.outMsgChan <- &bulkSetMsg{
				vs:     vs,
				header: make([]byte, 8),
				body:   make([]byte, _GLH_OUT_BULK_SET_MSG_SIZE),
			}
		}
	}
}

func (vs *DefaultValueStore) inBulkSet() {
	for {
		bsm := <-vs.bulkSetState.inMsgChan
		var bsam *bulkSetAckMsg
		if bsm.nodeID() != 0 {
			bsam = vs.newOutBulkSetAckMsg()
		}
		body := bsm.body
		var err error
		ring := vs.msgRing.Ring()
		ringVersion := ring.Version()
		rightwardPartitionShift := 64 - ring.PartitionBitCount()
		for len(body) > 0 {
			keyA := binary.BigEndian.Uint64(body)
			keyB := binary.BigEndian.Uint64(body[8:])
			timestampbits := binary.BigEndian.Uint64(body[16:])
			l := binary.BigEndian.Uint32(body[24:])
			_, err = vs.write(keyA, keyB, timestampbits, body[28:28+l])
			if bsam != nil && err == nil {
				if ringVersion != ring.Version() {
					ringVersion = ring.Version()
					rightwardPartitionShift = 64 - ring.PartitionBitCount()
				}
				if ring.Responsible(uint32(keyA >> rightwardPartitionShift)) {
					bsam.add(keyA, keyB, timestampbits)
				}
			}
			body = body[28+l:]
		}
		if bsam != nil {
			vs.msgRing.MsgToNode(bsm.nodeID(), bsam)
		}
		vs.bulkSetState.inFreeMsgChan <- bsm
	}
}

func (vs *DefaultValueStore) newInBulkSetMsg(r io.Reader, l uint64) (uint64, error) {
	var bsm *bulkSetMsg
	select {
	case bsm = <-vs.bulkSetState.inFreeMsgChan:
	case <-time.After(_GLH_IN_BULK_SET_MSG_TIMEOUT * time.Second):
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
	if l < 8 {
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
	var n int
	var sn int
	var err error
	for n != len(bsm.header) {
		sn, err = r.Read(bsm.header[n:])
		n += sn
		if err != nil {
			return uint64(n), err
		}
	}
	l -= 8
	if l > uint64(cap(bsm.body)) {
		bsm.body = make([]byte, l)
	}
	bsm.body = bsm.body[:l]
	n = 0
	for n != len(bsm.body) {
		sn, err = r.Read(bsm.body[n:])
		n += sn
		if err != nil {
			return uint64(n), err
		}
	}
	vs.bulkSetState.inMsgChan <- bsm
	return l, nil
}

func (vs *DefaultValueStore) newOutBulkSetMsg() *bulkSetMsg {
	bsm := <-vs.bulkSetState.outMsgChan
	binary.BigEndian.PutUint64(bsm.header, 0)
	bsm.body = bsm.body[:0]
	return bsm
}

func (bsm *bulkSetMsg) MsgType() uint64 {
	return _MSG_BULK_SET
}

func (bsm *bulkSetMsg) MsgLength() uint64 {
	return uint64(8 + len(bsm.body))
}

func (bsm *bulkSetMsg) WriteContent(w io.Writer) (uint64, error) {
	n, err := w.Write(bsm.header)
	if err != nil {
		return uint64(n), err
	}
	n, err = w.Write(bsm.body)
	return uint64(8 + n), err
}

func (bsm *bulkSetMsg) Done() {
	bsm.vs.bulkSetState.outMsgChan <- bsm
}

func (bsm *bulkSetMsg) nodeID() uint64 {
	return binary.BigEndian.Uint64(bsm.header)
}

func (bsm *bulkSetMsg) add(keyA uint64, keyB uint64, timestampbits uint64, value []byte) bool {
	o := len(bsm.body)
	if o+len(value)+28 >= cap(bsm.body) {
		return false
	}
	bsm.body = bsm.body[:o+len(value)+28]
	binary.BigEndian.PutUint64(bsm.body[o:], keyA)
	binary.BigEndian.PutUint64(bsm.body[o+8:], keyB)
	binary.BigEndian.PutUint64(bsm.body[o+16:], timestampbits)
	binary.BigEndian.PutUint32(bsm.body[o+24:], uint32(len(value)))
	copy(bsm.body[o+28:], value)
	return true
}
