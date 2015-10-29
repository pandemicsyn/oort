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
	l := log.New(os.Stdout, "DebugStore ", log.LstdFlags)
	s.o.ValueStoreConfig.LogDebug = l.Printf
	s.start()
	s.stopped = false
	return s
}

func (vsc *OortStore) start() {
	var err error
	log.Println("LocalID appears to be:", vsc.o.GetLocalID())
	vsc.t = ring.NewTCPMsgRing(nil)
	vsc.o.ValueStoreConfig.MsgRing = vsc.t
	vsc.t.SetRing(vsc.o.Ring())
	vsc.vs, err = valuestore.NewValueStore(&vsc.o.ValueStoreConfig)
	if err != nil {
		panic(err)
	}
	vsc.vs.EnableAll()
	go func() {
		vsc.t.Listen()
		log.Println("Listen() returned, shutdown?")
	}()
	go func() {
		tcpMsgRingStats := vsc.t.Stats(false)
		for !tcpMsgRingStats.Shutdown {
			time.Sleep(time.Minute)
			tcpMsgRingStats = vsc.t.Stats(false)
			log.Printf("%v\n", tcpMsgRingStats)
			log.Printf("%s\n", vsc.vs.Stats(false))
		}
	}()
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

func (vsc *OortStore) Del(key []byte) {
	keyA, keyB := murmur3.Sum128(key)
	var err error
	_, err = vsc.vs.Delete(keyA, keyB, brimtime.TimeToUnixMicro(time.Now()))
	if err != nil {
		log.Printf("Del: %#v %s\n", string(key), err)
	}
}

func (vsc *OortStore) Start() {
	vsc.Lock()
	if !vsc.stopped {
		vsc.Unlock()
		return
	}
	vsc.start()
	vsc.stopped = false
	vsc.Unlock()
	log.Println(vsc.vs.Stats(true))
	log.Println("Ortstore stop complete")
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
	vsc.stopped = true
	vsc.Unlock()
	log.Println(vsc.vs.Stats(true))
	log.Println("Ortstore stop complete")
}

func (vsc *OortStore) Stats() []byte {
	return []byte(vsc.vs.Stats(true).String())
}
