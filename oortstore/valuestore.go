package oortstore

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"log"
	"net"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/gholt/ring"
	"github.com/gholt/store"
	"github.com/pandemicsyn/oort/oort"
	"github.com/pandemicsyn/oort/rediscache"
	"github.com/spaolacci/murmur3"
	"gopkg.in/gholt/brimtime.v1"
)

type OortValueStore struct {
	sync.RWMutex
	waitGroup        *sync.WaitGroup
	vs               store.ValueStore
	t                *ring.TCPMsgRing
	o                *oort.Server
	C                *OortValueConfig `toml:"OortValueStoreConfig"` // load config using an explicit/different config header
	ch               chan bool        //channel to signal stop
	stopped          bool
	ValueStoreConfig store.ValueStoreConfig
	TCPMsgRingConfig ring.TCPMsgRingConfig
	serverTLSConfig  *tls.Config
}

type OortValueConfig struct {
	Debug      bool
	Profile    bool
	ListenAddr string `toml:"ListenAddress"` //another example
	MaxClients int
}

func NewValueStore(oort *oort.Server) (*OortValueStore, error) {
	s := &OortValueStore{}
	s.C = &OortValueConfig{}
	s.waitGroup = &sync.WaitGroup{}
	s.ch = make(chan bool)
	s.o = oort
	err := s.o.LoadRingConfig(s)
	if err != nil {
		return s, err
	}
	if s.C.Debug {
		log.Println("Ring entries:")
		ring := s.o.Ring()
		for k, _ := range ring.Nodes() {
			log.Println(ring.Nodes()[k].ID(), ring.Nodes()[k].Addresses())
		}
	}
	l := log.New(os.Stdout, "DebugStore ", log.LstdFlags)
	s.ValueStoreConfig.LogDebug = l.Printf
	if s.TCPMsgRingConfig.UseTLS {
		log.Println("TCPMsgRing using TLS")
	}
	s.start()
	s.stopped = false
	return s, nil
}

func (s *OortValueStore) start() {
	var err error
	s.vs = nil
	runtime.GC()
	log.Println("LocalID appears to be:", s.o.GetLocalID())
	s.t = ring.NewTCPMsgRing(&s.TCPMsgRingConfig)
	s.ValueStoreConfig.MsgRing = s.t
	s.t.SetRing(s.o.Ring())
	s.vs, err = store.NewValueStore(&s.ValueStoreConfig)
	if err != nil {
		panic(err)
	}
	s.vs.EnableAll()
	go func(t *ring.TCPMsgRing) {
		t.Listen()
		log.Println("Listen() returned, shutdown?")
	}(s.t)
	go func(t *ring.TCPMsgRing) {
		tcpMsgRingStats := t.Stats(false)
		for !tcpMsgRingStats.Shutdown {
			time.Sleep(time.Minute)
			tcpMsgRingStats = t.Stats(false)
			log.Printf("%v\n", tcpMsgRingStats)
			log.Printf("%s\n", s.vs.Stats(false))
		}
	}(s.t)
}

func (s *OortValueStore) UpdateRing(ring ring.Ring) {
	s.Lock()
	s.t.SetRing(ring)
	s.Unlock()
	log.Println("Oortstore updated tcp msg ring.")
}

func (s *OortValueStore) Get(key []byte, value []byte) []byte {
	keyA, keyB := murmur3.Sum128(key)
	var err error
	_, value, err = s.vs.Read(keyA, keyB, value)
	if err != nil {
		log.Printf("Get: %#v %s\n", string(key), err)
	}
	return value
}

func (s *OortValueStore) Set(key []byte, value []byte) {
	if bytes.Equal(key, rediscache.BYTES_SHUTDOWN) && bytes.Equal(value, rediscache.BYTES_NOW) {
		s.vs.DisableAll()
		s.vs.Flush()
		log.Println(s.vs.Stats(true))
		os.Exit(0)
		return
	}
	keyA, keyB := murmur3.Sum128(key)
	_, err := s.vs.Write(keyA, keyB, brimtime.TimeToUnixMicro(time.Now()), value)
	if err != nil {
		panic(err)
	}
}

func (s *OortValueStore) Del(key []byte) {
	keyA, keyB := murmur3.Sum128(key)
	var err error
	_, err = s.vs.Delete(keyA, keyB, brimtime.TimeToUnixMicro(time.Now()))
	if err != nil {
		log.Printf("Del: %#v %s\n", string(key), err)
	}
}

func (s *OortValueStore) Start() {
	s.Lock()
	if !s.stopped {
		s.Unlock()
		return
	}
	s.start()
	s.stopped = false
	s.Unlock()
	log.Println(s.vs.Stats(true))
	log.Println("ValueStore start complete")
}

func (s *OortValueStore) Stop() {
	s.Lock()
	if s.stopped {
		s.Unlock()
		return
	}
	s.vs.DisableAll()
	s.vs.Flush()
	s.t.Shutdown()
	s.stopped = true
	s.Unlock()
	log.Println(s.vs.Stats(true))
	log.Println("Ortstore stop complete")
}

func (s *OortValueStore) Stats() []byte {
	return []byte(s.vs.Stats(true).String())
}

// TODO: need to reimplement graceful shutdown
func (s *OortValueStore) handle_conn(conn net.Conn, handler *rediscache.RESPhandler) {
	defer conn.Close()
	defer s.waitGroup.Done()
	for {
		err := handler.Parse()
		if err != nil {
			return
		}
	}
}

func (s *OortValueStore) ListenAndServe() {
	s.ch = make(chan bool)
	readerChan := make(chan *bufio.Reader, s.C.MaxClients)
	for i := 0; i < cap(readerChan); i++ {
		readerChan <- nil
	}
	writerChan := make(chan *bufio.Writer, s.C.MaxClients)
	for i := 0; i < cap(writerChan); i++ {
		writerChan <- nil
	}
	handlerChan := make(chan *rediscache.RESPhandler, s.C.MaxClients)
	for i := 0; i < cap(handlerChan); i++ {
		handlerChan <- rediscache.NewRESPhandler(s)
	}
	addr, err := net.ResolveTCPAddr("tcp", s.C.ListenAddr)
	if err != nil {
		log.Println("Error getting IP: ", err)
		return
	}
	server, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Println("Error starting: ", err)
		return
	}
	log.Println("Listening on:", s.C.ListenAddr)
	for {
		select {
		case <-s.ch:
			log.Println("ListenAndServe Shutting down")
			server.Close()
			return
		default:
		}
		server.SetDeadline(time.Now().Add(1e9))
		var conn net.Conn
		if s.serverTLSConfig != nil {
			l := tls.NewListener(server, s.serverTLSConfig)
			conn, err = l.Accept()
		} else {
			conn, err = server.AcceptTCP()
		}
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}
			log.Println(err)
		}
		reader := <-readerChan
		if reader == nil {
			reader = bufio.NewReaderSize(conn, 65536)
		} else {
			reader.Reset(conn)
		}
		writer := <-writerChan
		if writer == nil {
			writer = bufio.NewWriterSize(conn, 65536)
		} else {
			writer.Reset(conn)
		}
		handler := <-handlerChan
		handler.Reset(reader, writer)
		s.waitGroup.Add(1)
		go s.handle_conn(conn, handler)
		handlerChan <- handler
		writerChan <- writer
		readerChan <- reader
	}

}

func (s *OortValueStore) StopListenAndServe() {
	close(s.ch)
}

func (s *OortValueStore) Wait() {
	s.waitGroup.Wait()
	return
}
