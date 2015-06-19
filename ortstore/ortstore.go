package ortstore

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/gholt/brimtime"
	"github.com/gholt/ring"
	"github.com/gholt/valuestore"
	"github.com/pandemicsyn/ort/rediscache"
	"github.com/spaolacci/murmur3"
)

type OrtStore struct {
	vs    valuestore.ValueStore
	rfile string
}

func getMsgRing(filename string) (ring.MsgRing, error) {
	var f *os.File
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	r, err := ring.LoadRing(f)
	return ring.NewTCPMsgRing(r), nil
}

func New(rfile string) *OrtStore {
	mr, err := getMsgRing(rfile)
	if err != nil {
		panic(err)
	}
	s := &OrtStore{vs: valuestore.New(valuestore.OptMsgRing(mr))}
	s.vs.EnableAll()
	return s
}

func (vsc *OrtStore) Get(key []byte, value []byte) []byte {
	keyA, keyB := murmur3.Sum128(key)
	var err error
	_, value, err = vsc.vs.Read(keyA, keyB, value)
	if err != nil {
		fmt.Printf("Get: %#v %s\n", string(key), err)
	}
	return value
}

func (vsc *OrtStore) Set(key []byte, value []byte) {
	if bytes.Equal(key, rediscache.BYTES_SHUTDOWN) && bytes.Equal(value, rediscache.BYTES_NOW) {
		vsc.vs.DisableAll()
		vsc.vs.Flush()
		fmt.Println(vsc.vs.GatherStats(true))
		//pprof.StopCPUProfile()
		//pproffp.Close()
		os.Exit(0)
		return
	}
	keyA, keyB := murmur3.Sum128(key)
	_, err := vsc.vs.Write(keyA, keyB, brimtime.TimeToUnixMicro(time.Now()), value)
	if err != nil {
		panic(err)
	}
}
