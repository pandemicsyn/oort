package valuestore

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spaolacci/murmur3"
	"gopkg.in/gholt/brimutil.v1"
)

type valuesFile struct {
	vs                  *DefaultValueStore
	id                  uint32
	bts                 int64
	writerFP            io.WriteCloser
	atOffset            uint32
	freeChan            chan *valuesFileWriteBuf
	checksumChan        chan *valuesFileWriteBuf
	writeChan           chan *valuesFileWriteBuf
	doneChan            chan struct{}
	buf                 *valuesFileWriteBuf
	freeableVMChanIndex int
	readerFPs           []brimutil.ChecksummedReader
	readerLocks         []sync.Mutex
	readerLens          [][]byte
}

type valuesFileWriteBuf struct {
	seq    int
	buf    []byte
	offset uint32
	vms    []*valuesMem
}

func newValuesFile(vs *DefaultValueStore, bts int64) *valuesFile {
	vf := &valuesFile{vs: vs, bts: bts}
	name := path.Join(vs.path, fmt.Sprintf("%d.values", vf.bts))
	vf.readerFPs = make([]brimutil.ChecksummedReader, vs.valuesFileReaders)
	vf.readerLocks = make([]sync.Mutex, len(vf.readerFPs))
	vf.readerLens = make([][]byte, len(vf.readerFPs))
	for i := 0; i < len(vf.readerFPs); i++ {
		fp, err := os.Open(name)
		if err != nil {
			panic(err)
		}
		vf.readerFPs[i] = brimutil.NewChecksummedReader(fp, int(vs.checksumInterval), murmur3.New32)
		vf.readerLens[i] = make([]byte, 4)
	}
	vf.id = vs.addValueLocBlock(vf)
	return vf
}

func createValuesFile(vs *DefaultValueStore) *valuesFile {
	vf := &valuesFile{vs: vs, bts: time.Now().UnixNano()}
	name := path.Join(vs.path, fmt.Sprintf("%d.values", vf.bts))
	fp, err := os.Create(name)
	if err != nil {
		panic(err)
	}
	vf.writerFP = fp
	vf.freeChan = make(chan *valuesFileWriteBuf, vs.workers)
	for i := 0; i < vs.workers; i++ {
		vf.freeChan <- &valuesFileWriteBuf{buf: make([]byte, vs.checksumInterval+4)}
	}
	vf.checksumChan = make(chan *valuesFileWriteBuf, vs.workers)
	vf.writeChan = make(chan *valuesFileWriteBuf, vs.workers)
	vf.doneChan = make(chan struct{})
	vf.buf = <-vf.freeChan
	head := []byte("VALUESTORE v0                   ")
	binary.BigEndian.PutUint32(head[28:], vs.checksumInterval)
	vf.buf.offset = uint32(copy(vf.buf.buf, head))
	atomic.StoreUint32(&vf.atOffset, vf.buf.offset)
	go vf.writer()
	for i := 0; i < vs.workers; i++ {
		go vf.checksummer()
	}
	vf.readerFPs = make([]brimutil.ChecksummedReader, vs.valuesFileReaders)
	vf.readerLocks = make([]sync.Mutex, len(vf.readerFPs))
	vf.readerLens = make([][]byte, len(vf.readerFPs))
	for i := 0; i < len(vf.readerFPs); i++ {
		fp, err := os.Open(name)
		if err != nil {
			panic(err)
		}
		vf.readerFPs[i] = brimutil.NewChecksummedReader(fp, int(vs.checksumInterval), murmur3.New32)
		vf.readerLens[i] = make([]byte, 4)
	}
	vf.id = vs.addValueLocBlock(vf)
	return vf
}

func (vf *valuesFile) timestampnano() int64 {
	return vf.bts
}

func (vf *valuesFile) read(keyA uint64, keyB uint64, timestampbits uint64, offset uint32, length uint32, value []byte) (uint64, []byte, error) {
	if timestampbits&_TSB_DELETION != 0 {
		return timestampbits, value, ErrNotFound
	}
	i := int(keyA>>1) % len(vf.readerFPs)
	vf.readerLocks[i].Lock()
	vf.readerFPs[i].Seek(int64(offset), 0)
	end := len(value) + int(length)
	if end <= cap(value) {
		value = value[:end]
	} else {
		value2 := make([]byte, end)
		copy(value2, value)
		value = value2
	}
	if _, err := io.ReadFull(vf.readerFPs[i], value[len(value)-int(length):]); err != nil {
		vf.readerLocks[i].Unlock()
		return timestampbits, value, err
	}
	vf.readerLocks[i].Unlock()
	return timestampbits, value, nil
}

func (vf *valuesFile) write(vm *valuesMem) {
	if vm == nil {
		return
	}
	vm.vfID = vf.id
	vm.vfOffset = atomic.LoadUint32(&vf.atOffset)
	if len(vm.values) < 1 {
		vf.vs.freeableVMChans[vf.freeableVMChanIndex] <- vm
		vf.freeableVMChanIndex++
		if vf.freeableVMChanIndex >= len(vf.vs.freeableVMChans) {
			vf.freeableVMChanIndex = 0
		}
		return
	}
	left := len(vm.values)
	for left > 0 {
		n := copy(vf.buf.buf[vf.buf.offset:vf.vs.checksumInterval], vm.values[len(vm.values)-left:])
		atomic.AddUint32(&vf.atOffset, uint32(n))
		vf.buf.offset += uint32(n)
		if vf.buf.offset >= vf.vs.checksumInterval {
			s := vf.buf.seq
			vf.checksumChan <- vf.buf
			vf.buf = <-vf.freeChan
			vf.buf.seq = s + 1
		}
		left -= n
	}
	if vf.buf.offset == 0 {
		vf.vs.freeableVMChans[vf.freeableVMChanIndex] <- vm
		vf.freeableVMChanIndex++
		if vf.freeableVMChanIndex >= len(vf.vs.freeableVMChans) {
			vf.freeableVMChanIndex = 0
		}
	} else {
		vf.buf.vms = append(vf.buf.vms, vm)
	}
}

func (vf *valuesFile) close() {
	close(vf.checksumChan)
	for i := 0; i < cap(vf.checksumChan); i++ {
		<-vf.doneChan
	}
	vf.writeChan <- nil
	<-vf.doneChan
	term := make([]byte, 16)
	binary.BigEndian.PutUint64(term[4:], uint64(atomic.LoadUint32(&vf.atOffset)))
	copy(term[12:], "TERM")
	left := len(term)
	for left > 0 {
		n := copy(vf.buf.buf[vf.buf.offset:vf.vs.checksumInterval], term[len(term)-left:])
		vf.buf.offset += uint32(n)
		binary.BigEndian.PutUint32(vf.buf.buf[vf.buf.offset:], murmur3.Sum32(vf.buf.buf[:vf.buf.offset]))
		if _, err := vf.writerFP.Write(vf.buf.buf[:vf.buf.offset+4]); err != nil {
			panic(err)
		}
		vf.buf.offset = 0
		left -= n
	}
	if err := vf.writerFP.Close(); err != nil {
		panic(err)
	}
	for _, vm := range vf.buf.vms {
		vf.vs.freeableVMChans[vf.freeableVMChanIndex] <- vm
		vf.freeableVMChanIndex++
		if vf.freeableVMChanIndex >= len(vf.vs.freeableVMChans) {
			vf.freeableVMChanIndex = 0
		}
	}
	vf.writerFP = nil
	vf.freeChan = nil
	vf.checksumChan = nil
	vf.writeChan = nil
	vf.doneChan = nil
	vf.buf = nil
}

func (vf *valuesFile) checksummer() {
	for {
		buf := <-vf.checksumChan
		if buf == nil {
			break
		}
		binary.BigEndian.PutUint32(buf.buf[vf.vs.checksumInterval:], murmur3.Sum32(buf.buf[:vf.vs.checksumInterval]))
		vf.writeChan <- buf
	}
	vf.doneChan <- struct{}{}
}

func (vf *valuesFile) writer() {
	var seq int
	lastWasNil := false
	for {
		buf := <-vf.writeChan
		if buf == nil {
			if lastWasNil {
				break
			}
			lastWasNil = true
			vf.writeChan <- nil
			continue
		}
		lastWasNil = false
		if buf.seq != seq {
			vf.writeChan <- buf
			continue
		}
		if _, err := vf.writerFP.Write(buf.buf); err != nil {
			panic(err)
		}
		if len(buf.vms) > 0 {
			for _, vm := range buf.vms {
				vf.vs.freeableVMChans[vf.freeableVMChanIndex] <- vm
				vf.freeableVMChanIndex++
				if vf.freeableVMChanIndex >= len(vf.vs.freeableVMChans) {
					vf.freeableVMChanIndex = 0
				}
			}
			buf.vms = buf.vms[:0]
		}
		buf.offset = 0
		vf.freeChan <- buf
		seq++
	}
	vf.doneChan <- struct{}{}
}
