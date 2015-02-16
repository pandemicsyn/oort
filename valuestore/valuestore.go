package valuestore

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/gholt/brimtime"
	"github.com/gholt/valuestore"
	"github.com/pandemicsyn/ort/rediscache"
	"github.com/spaolacci/murmur3"
)

type ValueStore struct {
	vs valuestore.ValueStore
}

func New() *ValueStore {
	vs := &ValueStore{vs: valuestore.New()}
	vs.vs.EnableAll()
	return vs
}

func (vsc *ValueStore) Get(key []byte, value []byte) []byte {
	keyA, keyB := murmur3.Sum128(key)
	var err error
	_, value, err = vsc.vs.Read(keyA, keyB, value)
	if err != nil {
		fmt.Printf("Get: %#v %s\n", string(key), err)
	}
	return value
}

func (vsc *ValueStore) Set(key []byte, value []byte) {
	//fmt.Printf("Set: %#v\n", string(key))
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
