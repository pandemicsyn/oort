package ortstore

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gholt/ring"
	"github.com/gholt/valuestore"
	"github.com/pandemicsyn/ort/rediscache"
	"github.com/spaolacci/murmur3"
	"gopkg.in/gholt/brimtime.v1"
)

type OrtStore struct {
	vs    valuestore.ValueStore
	rfile string
	r     ring.Ring
}

func getMsgRing(filename string) (ring.MsgRing, error) {
	var f *os.File
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	r, err := ring.LoadRing(f)
	tr := ring.NewTCPMsgRing(nil)
	tr.SetRing(r)
	return tr, nil
}

func New(rfile string, localid int) *OrtStore {
	//mr, err := getMsgRing(rfile)

	s := &OrtStore{}
	s.rfile = rfile

	f, err := os.Open(rfile)
	if err != nil {
		panic(err)
	}
	s.r, err = ring.LoadRing(f)
	if err != nil {
		panic(err)
	}
	fmt.Println("Ring entries:")
	for k, _ := range s.r.Nodes() {
		fmt.Println(s.r.Nodes()[k].ID(), s.r.Nodes()[k].Addresses())
	}
	fmt.Println("Pretending to be:", s.r.Nodes()[localid].ID(), s.r.Nodes()[localid].Addresses())
	s.r.SetLocalNode(s.r.Nodes()[localid].ID())
	t := ring.NewTCPMsgRing(nil)
	t.SetRing(s.r)
	l := log.New(os.Stdout, "DebugStore ", log.LstdFlags)
	s.vs = valuestore.New(&valuestore.Config{MsgRing: t, LogDebug: l})
	s.vs.EnableAll()
	go func() {
		t.Listen()
		log.Println("Listen() returned, shutdown?")
	}()
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
