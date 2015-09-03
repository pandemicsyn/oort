package ortstore

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gholt/brimtime"
	"github.com/gholt/ring"
	"github.com/gholt/valuestore"
	"github.com/pandemicsyn/ort/rediscache"
	"github.com/spaolacci/murmur3"
)

type OrtStore struct {
	vs      valuestore.ValueStore
	r       ring.Ring
	rfile   string
	localid uint64
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

func New(ortring ring.Ring, ringfile string, localid uint64) *OrtStore {
	s := &OrtStore{}
	s.r = ortring
	s.rfile = ringfile
	s.localid = localid

	fmt.Println("Ring entries:")
	for k, _ := range s.r.Nodes() {
		fmt.Println(s.r.Nodes()[k].ID(), s.r.Nodes()[k].Addresses())
	}
	fmt.Println("Localid appears to be:", s.localid)
	s.r.SetLocalNode(s.localid)
	node := s.r.LocalNode()
	log.Printf("%#v\n", node)
	log.Println("Wat:", node.Addresses())
	t := ring.NewTCPMsgRing(s.r)
	l := log.New(os.Stdout, "DebugStore ", log.LstdFlags)
	s.vs = valuestore.New(&valuestore.Config{MsgRing: t, LogDebug: l})
	s.vs.EnableAll()
	go func() {
		chanerr := t.Start()
		err := <-chanerr
		if err != nil {
			log.Fatal(err)
		} else {
			log.Println("Start() sent nil, shutdown?")
		}

	}()
	go func() {
		time.Sleep(10 * time.Second)
		log.Println("triggering stop")
		t.Stop()
		log.Println("stop returned!")
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
