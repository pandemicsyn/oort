package valuestore

import (
	"encoding/binary"
	"io"
	"time"
)

const _GLH_IN_BULK_SET_ACK_MSGS = 128
const _GLH_IN_BULK_SET_ACK_HANDLERS = 40
const _GLH_OUT_BULK_SET_ACK_MSGS = 128
const _GLH_OUT_BULK_SET_ACK_MSG_SIZE = 16 * 1024 * 1024
const _GLH_IN_BULK_SET_ACK_MSG_TIMEOUT = 300
const _MSG_BULK_SET_ACK = 0x39589f4746844e3b

type bulkSetAckState struct {
	inMsgChan      chan *bulkSetAckMsg
	inFreeMsgChan  chan *bulkSetAckMsg
	outFreeMsgChan chan *bulkSetAckMsg
}

type bulkSetAckMsg struct {
	vs   *DefaultValueStore
	body []byte
}

func (vs *DefaultValueStore) bulkSetAckInit(cfg *config) {
	if vs.msgRing != nil {
		vs.msgRing.SetMsgHandler(_MSG_BULK_SET_ACK, vs.newInBulkSetAckMsg)
		vs.bulkSetAckState.inMsgChan = make(chan *bulkSetAckMsg, _GLH_IN_BULK_SET_ACK_MSGS)
		vs.bulkSetAckState.inFreeMsgChan = make(chan *bulkSetAckMsg, _GLH_IN_BULK_SET_ACK_MSGS)
		for i := 0; i < cap(vs.bulkSetAckState.inFreeMsgChan); i++ {
			vs.bulkSetAckState.inFreeMsgChan <- &bulkSetAckMsg{vs: vs}
		}
		for i := 0; i < _GLH_IN_BULK_SET_ACK_HANDLERS; i++ {
			go vs.inBulkSetAck()
		}
		vs.bulkSetAckState.outFreeMsgChan = make(chan *bulkSetAckMsg, _GLH_OUT_BULK_SET_ACK_MSGS)
		for i := 0; i < cap(vs.bulkSetAckState.outFreeMsgChan); i++ {
			vs.bulkSetAckState.outFreeMsgChan <- &bulkSetAckMsg{
				vs:   vs,
				body: make([]byte, _GLH_OUT_BULK_SET_ACK_MSG_SIZE),
			}
		}
	}
}

func (vs *DefaultValueStore) inBulkSetAck() {
	for {
		bsam := <-vs.bulkSetAckState.inMsgChan
		ring := vs.msgRing.Ring()
		version := ring.Version()
		rightwardPartitionShift := 64 - ring.PartitionBitCount()
		b := bsam.body
		l := len(b)
		for o := 0; o < l; o += 24 {
			if version != ring.Version() {
				version = ring.Version()
				rightwardPartitionShift = 64 - ring.PartitionBitCount()
			}
			keyA := binary.BigEndian.Uint64(b[o:])
			if !ring.Responsible(uint32(keyA >> rightwardPartitionShift)) {
				vs.write(keyA, binary.BigEndian.Uint64(b[o+8:]), binary.BigEndian.Uint64(b[o+16:])|_TSB_LOCAL_REMOVAL, nil)
			}
		}
		vs.bulkSetAckState.inFreeMsgChan <- bsam
	}
}

func (vs *DefaultValueStore) newInBulkSetAckMsg(r io.Reader, l uint64) (uint64, error) {
	var bsam *bulkSetAckMsg
	select {
	case bsam = <-vs.bulkSetAckState.inFreeMsgChan:
	case <-time.After(_GLH_IN_BULK_SET_ACK_MSG_TIMEOUT * time.Second):
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
	if l > uint64(cap(bsam.body)) {
		bsam.body = make([]byte, l)
	}
	bsam.body = bsam.body[:l]
	n = 0
	for n != len(bsam.body) {
		sn, err = r.Read(bsam.body[n:])
		n += sn
		if err != nil {
			return uint64(n), err
		}
	}
	vs.bulkSetAckState.inMsgChan <- bsam
	return l, nil
}

func (vs *DefaultValueStore) newOutBulkSetAckMsg() *bulkSetAckMsg {
	bsam := <-vs.bulkSetAckState.outFreeMsgChan
	bsam.body = bsam.body[:0]
	return bsam
}

func (bsam *bulkSetAckMsg) MsgType() uint64 {
	return _MSG_BULK_SET_ACK
}

func (bsam *bulkSetAckMsg) MsgLength() uint64 {
	return uint64(len(bsam.body))
}

func (bsam *bulkSetAckMsg) WriteContent(w io.Writer) (uint64, error) {
	n, err := w.Write(bsam.body)
	return uint64(n), err
}

func (bsam *bulkSetAckMsg) Done() {
	bsam.vs.bulkSetAckState.outFreeMsgChan <- bsam
}

func (bsam *bulkSetAckMsg) add(keyA uint64, keyB uint64, timestampbits uint64) bool {
	o := len(bsam.body)
	if o+24 >= cap(bsam.body) {
		return false
	}
	bsam.body = bsam.body[:o+24]
	binary.BigEndian.PutUint64(bsam.body[o:], keyA)
	binary.BigEndian.PutUint64(bsam.body[o+8:], keyB)
	binary.BigEndian.PutUint64(bsam.body[o+16:], timestampbits)
	return true
}
