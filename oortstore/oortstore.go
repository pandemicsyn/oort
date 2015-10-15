package oortstore

import (
	"bytes"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gholt/ring"
	"github.com/gholt/valuestore"
	"github.com/pandemicsyn/oort/oort"
	"github.com/pandemicsyn/oort/rediscache"
	"github.com/spaolacci/murmur3"
	"gopkg.in/gholt/brimtime.v1"
)

type OortStore struct {
	sync.RWMutex
	vs      valuestore.ValueStore
	t       *ring.TCPMsgRing
	o       *oort.Server
	c       *Config
	stopped bool
}

type Config struct {
	Debug   bool
	Profile bool
}

func New(oort *oort.Server, config *Config) *OortStore {
	s := &OortStore{}
	s.o = oort
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

func (vsc *OortStore) UpdateRing(ring ring.Ring) {
	vsc.Lock()
	vsc.t.SetRing(ring)
	vsc.Unlock()
	log.Println("Oortstore updated tcp msg ring.")
}

func (vsc *OortStore) Get(key []byte, value []byte) []byte {
	keyA, keyB := murmur3.Sum128(key)
	var err error
	_, value, err = vsc.vs.Read(keyA, keyB, value)
	if err != nil {
		log.Printf("Get: %#v %s\n", string(key), err)
	}
	return value
}

func (vsc *OortStore) Set(key []byte, value []byte) {
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

func (vsc *OortStore) Stop() {
	vsc.Lock()
	if vsc.stopped {
		vsc.Unlock()
		return
	}
	vsc.vs.DisableAll()
	vsc.vs.Flush()
	vsc.t.Shutdown()
	vsc.Unlock()
	log.Println(vsc.vs.Stats(true))
	log.Println("Ortstore stop complete")
}

func (vsc *OortStore) Stats() []byte {
	return []byte(vsc.vs.Stats(true).String())
}
