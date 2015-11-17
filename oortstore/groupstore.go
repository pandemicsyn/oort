package oortstore

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/gholt/ring"
	"github.com/gholt/store"
	"github.com/pandemicsyn/oort/oort"
	"github.com/pandemicsyn/oort/rediscache"
	"github.com/spaolacci/murmur3"
	"gopkg.in/gholt/brimtime.v1"
)

type OortGroupStore struct {
	sync.RWMutex
	waitGroup        *sync.WaitGroup
	vs               store.GroupStore
	t                *ring.TCPMsgRing
	o                *oort.Server
	C                *OortGroupConfig `toml:"OortGroupStoreConfig"` // load config using an explicit/different config header
	ch               chan bool        //channel to signal stop
	stopped          bool
	GroupStoreConfig store.GroupStoreConfig
	TCPMsgRingConfig ring.TCPMsgRingConfig
	serverTLSConfig  *tls.Config
}

type OortGroupConfig struct {
	Debug      bool
	Profile    bool
	ListenAddr string `toml:"ListenAddress"` //another example
	MaxClients int
}

func NewGroupStore(oort *oort.Server) (*OortGroupStore, error) {
	s := &OortGroupStore{}
	s.C = &OortGroupConfig{}
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
	s.GroupStoreConfig.LogDebug = l.Printf
	if s.TCPMsgRingConfig.UseTLS {
		log.Println("TCPMsgRing using TLS")
	}
	s.start()
	s.stopped = false
	return s, nil
}

func (s *OortGroupStore) start() {
	var err error
	log.Println("LocalID appears to be:", s.o.GetLocalID())
	s.t = ring.NewTCPMsgRing(&s.TCPMsgRingConfig)
	s.GroupStoreConfig.MsgRing = s.t
	s.t.SetRing(s.o.Ring())
	s.vs, err = store.NewGroupStore(&s.GroupStoreConfig)
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
}

func (s *OortGroupStore) UpdateRing(ring ring.Ring) {
	s.Lock()
	s.t.SetRing(ring)
	s.Unlock()
	log.Println("Oortstore updated tcp msg ring.")
}

// TODO: fix me, just need to build for now, blindly setting namekeya/b to key!
func (s *OortGroupStore) Get(key []byte, value []byte) []byte {
	keyA, keyB := murmur3.Sum128(key)
	nameKeyA, nameKeyB := murmur3.Sum128(key)
	var err error
	_, value, err = s.vs.Read(keyA, keyB, nameKeyA, nameKeyB, value)
	if err != nil {
		log.Printf("Get: %#v %s\n", string(key), err)
	}
	return value
}

// TODO: fix me, just need to build for now, blindly setting namekeya/b to key!
func (s *OortGroupStore) Set(key []byte, value []byte) {
	if bytes.Equal(key, rediscache.BYTES_SHUTDOWN) && bytes.Equal(value, rediscache.BYTES_NOW) {
		s.vs.DisableAll()
		s.vs.Flush()
		log.Println(s.vs.Stats(true))
		os.Exit(0)
		return
	}
	keyA, keyB := murmur3.Sum128(key)
	nameKeyA, nameKeyB := murmur3.Sum128(key)
	_, err := s.vs.Write(keyA, keyB, nameKeyA, nameKeyB, brimtime.TimeToUnixMicro(time.Now()), value)
	if err != nil {
		panic(err)
	}
}

// TODO: fix me, just need to build for now, blindly setting namekeya/b to key!
func (s *OortGroupStore) Del(key []byte) {
	keyA, keyB := murmur3.Sum128(key)
	var err error
	nameKeyA, nameKeyB := murmur3.Sum128(key)
	_, err = s.vs.Delete(keyA, keyB, nameKeyA, nameKeyB, brimtime.TimeToUnixMicro(time.Now()))
	if err != nil {
		log.Printf("Del: %#v %s\n", string(key), err)
	}
}

func (s *OortGroupStore) Start() {
	s.Lock()
	if !s.stopped {
		s.Unlock()
		return
	}
	s.start()
	s.stopped = false
	s.Unlock()
	log.Println(s.vs.Stats(true))
	log.Println("GroupStore start complete")
}

func (s *OortGroupStore) Stop() {
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

func (s *OortGroupStore) Stats() []byte {
	return []byte(s.vs.Stats(true).String())
}

// TODO: need to reimplement graceful shutdown
func (s *OortGroupStore) handle_conn(conn net.Conn, handler *rediscache.RESPhandler) {
	defer conn.Close()
	defer s.waitGroup.Done()
	for {
		err := handler.Parse()
		if err != nil {
			return
		}
	}
}

func (s *OortGroupStore) ListenAndServe() {
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

func (s *OortGroupStore) StopListenAndServe() {
	close(s.ch)
}

func (s *OortGroupStore) Wait() {
	s.waitGroup.Wait()
	return
}
