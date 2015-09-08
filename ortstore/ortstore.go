package ortstore

import (
	"bytes"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gholt/brimtime"
	"github.com/gholt/ring"
	"github.com/gholt/valuestore"
	"github.com/pandemicsyn/ort/rediscache"
	"github.com/spaolacci/murmur3"
)

type OrtStore struct {
	sync.RWMutex
	vs      valuestore.ValueStore
	r       ring.Ring
	t       *ring.TCPMsgRing
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

	log.Println("Ring entries:")
	for k, _ := range s.r.Nodes() {
		log.Println(s.r.Nodes()[k].ID(), s.r.Nodes()[k].Addresses())
	}
	log.Println("Localid appears to be:", s.localid)
	s.r.SetLocalNode(s.localid)
	s.t = ring.NewTCPMsgRing(s.r)
	l := log.New(os.Stdout, "DebugStore ", log.LstdFlags)
	s.vs = valuestore.New(&valuestore.Config{MsgRing: s.t, LogDebug: l})
	s.vs.EnableAll()
	go func() {
		chanerr := s.t.Start()
		err := <-chanerr
		if err != nil {
			log.Fatal(err)
		} else {
			log.Println("Start() sent nil, shutdown?")
		}
	}()
	return s
}

func (vsc *OrtStore) Ring() ring.Ring {
	vsc.RLock()
	r := vsc.r
	vsc.RUnlock()
	return r
}

func (vsc *OrtStore) SetRing(n ring.Ring) {
	vsc.Lock()
	vsc.r = n
	vsc.t.SetRing(vsc.r)
	vsc.Unlock()
}

func (vsc *OrtStore) Get(key []byte, value []byte) []byte {
	keyA, keyB := murmur3.Sum128(key)
	var err error
	_, value, err = vsc.vs.Read(keyA, keyB, value)
	if err != nil {
		log.Printf("Get: %#v %s\n", string(key), err)
	}
	return value
}

func (vsc *OrtStore) Set(key []byte, value []byte) {
	if bytes.Equal(key, rediscache.BYTES_SHUTDOWN) && bytes.Equal(value, rediscache.BYTES_NOW) {
		vsc.vs.DisableAll()
		vsc.vs.Flush()
		log.Println(vsc.vs.GatherStats(true))
		os.Exit(0)
		return
	}
	keyA, keyB := murmur3.Sum128(key)
	_, err := vsc.vs.Write(keyA, keyB, brimtime.TimeToUnixMicro(time.Now()), value)
	if err != nil {
		panic(err)
	}
}
