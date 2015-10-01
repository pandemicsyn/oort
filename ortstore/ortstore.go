package ortstore

import (
	"bytes"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gholt/ring"
	"github.com/gholt/valuestore"
	"github.com/pandemicsyn/ort/ort"
	"github.com/pandemicsyn/ort/rediscache"
	"github.com/spaolacci/murmur3"
	"gopkg.in/gholt/brimtime.v1"
)

type OrtStore struct {
	sync.RWMutex
	vs valuestore.ValueStore
	t  *ring.TCPMsgRing
	o  *ort.Server
	c  *Config
}

type Config struct {
	Debug   bool
	Profile bool
}

func New(ort *ort.Server, config *Config) *OrtStore {
	s := &OrtStore{}
	s.o = ort
	s.c = config
	if s.c.Debug {
		log.Println("Ring entries:")
		ring := s.o.Ring()
		for k, _ := range ring.Nodes() {
			log.Println(ring.Nodes()[k].ID(), ring.Nodes()[k].Addresses())
		}
	}
	log.Println("LocalID appears to be:", s.o.GetLocalID())
	s.t = ring.NewTCPMsgRing(nil)
	s.t.SetRing(s.o.Ring())
	l := log.New(os.Stdout, "DebugStore ", log.LstdFlags)
	s.o.ValueStoreConfig.MsgRing = s.t
	s.o.ValueStoreConfig.LogDebug = l.Printf
	var err error
	s.vs, err = valuestore.New(&s.o.ValueStoreConfig)
	if err != nil {
		panic(err)
	}
	s.vs.EnableAll()
	go func() {
		s.t.Listen()
		log.Println("Listen() returned, shutdown?")
	}()
	go func() {
		tcpMsgRingStats := s.t.Stats(false)
		for !tcpMsgRingStats.Shutdown {
			time.Sleep(time.Minute)
			tcpMsgRingStats = s.t.Stats(false)
			log.Printf("%v\n", tcpMsgRingStats)
			log.Printf("%s\n", s.vs.Stats(false))
		}
	}()
	return s
}

func (vsc *OrtStore) UpdateRing() {
	vsc.Lock()
	vsc.t.SetRing(vsc.o.Ring())
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
		log.Println(vsc.vs.Stats(true))
		os.Exit(0)
		return
	}
	keyA, keyB := murmur3.Sum128(key)
	_, err := vsc.vs.Write(keyA, keyB, brimtime.TimeToUnixMicro(time.Now()), value)
	if err != nil {
		panic(err)
	}
}

func (vsc *OrtStore) Stop() {
	vsc.vs.DisableAll()
	vsc.vs.Flush()
	log.Println(vsc.vs.Stats(true))
}
