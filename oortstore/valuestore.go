package oortstore

import (
	"crypto/tls"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/gholt/ring"
	"github.com/gholt/store"
	"github.com/pandemicsyn/oort/api/valueproto"
	"github.com/pandemicsyn/oort/oort"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type OortValueStore struct {
	sync.RWMutex
	waitGroup        *sync.WaitGroup
	vs               store.ValueStore
	grpc             *grpc.Server
	grpcStopping     bool
	t                *ring.TCPMsgRing
	o                *oort.Server
	C                *OortValueConfig `toml:"OortValueStoreConfig"` // load config using an explicit/different config header
	stopped          bool
	ValueStoreConfig store.ValueStoreConfig
	TCPMsgRingConfig ring.TCPMsgRingConfig
	serverTLSConfig  *tls.Config
}

type OortValueConfig struct {
	Debug              bool
	Profile            bool
	ListenAddr         string `toml:"ListenAddress"` //another example
	InsecureSkipVerify bool
	CertFile           string
	KeyFile            string
}

func NewValueStore(oort *oort.Server) (*OortValueStore, error) {
	s := &OortValueStore{}
	s.C = &OortValueConfig{}
	s.waitGroup = &sync.WaitGroup{}
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
		l := log.New(os.Stdout, "DebugStore ", log.LstdFlags)
		s.ValueStoreConfig.LogDebug = l.Printf
	}
	if s.TCPMsgRingConfig.UseTLS {
		log.Println("TCPMsgRing using TLS")
	}
	cert, err := tls.LoadX509KeyPair(s.C.CertFile, s.C.KeyFile)
	if err != nil {
		return s, err
	}
	s.serverTLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: s.C.InsecureSkipVerify}
	s.start()
	s.stopped = false
	return s, nil
}

func (s *OortValueStore) start() {
	s.vs = nil
	runtime.GC()
	log.Println("LocalID appears to be:", s.o.GetLocalID())
	s.t = ring.NewTCPMsgRing(&s.TCPMsgRingConfig)
	s.ValueStoreConfig.MsgRing = s.t
	s.t.SetRing(s.o.Ring())
	var restartChan chan error
	s.vs, restartChan = store.NewValueStore(&s.ValueStoreConfig)
	// TODO: I'm guessing we'll want to do something more graceful here; but
	// this will work for now since Systemd (or another service manager) should
	// restart the service.
	go func(restartChan chan error) {
		if err := <-restartChan; err != nil {
			panic(err)
		}
	}(restartChan)
	if err := s.vs.Startup(); err != nil {
		panic(err)
	}
	go func(t *ring.TCPMsgRing) {
		t.Listen()
		log.Println("TCPMsgRing Listen() returned, shutdown?")
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

func (s *OortValueStore) Write(ctx context.Context, req *valueproto.WriteRequest) (*valueproto.WriteResponse, error) {
	resp := valueproto.WriteResponse{}
	var err error
	resp.TimestampMicro, err = s.vs.Write(req.KeyA, req.KeyB, req.TimestampMicro, req.Value)
	if err != nil {
		log.Println(err)
		resp.Err = err.Error()
	}
	return &resp, nil
}

func (s *OortValueStore) StreamWrite(stream valueproto.ValueStore_StreamWriteServer) error {
	var resp valueproto.WriteResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		resp.TimestampMicro, err = s.vs.Write(req.KeyA, req.KeyB, req.TimestampMicro, req.Value)
		if err != nil {
			log.Println(err)
			resp.Err = err.Error()
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *OortValueStore) Read(ctx context.Context, req *valueproto.ReadRequest) (*valueproto.ReadResponse, error) {
	resp := valueproto.ReadResponse{}
	var err error
	resp.TimestampMicro, resp.Value, err = s.vs.Read(req.KeyA, req.KeyB, resp.Value)
	if err != nil {
		resp.Err = err.Error()
	}
	return &resp, nil
}

func (s *OortValueStore) StreamRead(stream valueproto.ValueStore_StreamReadServer) error {
	var resp valueproto.ReadResponse

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		resp.TimestampMicro, resp.Value, err = s.vs.Read(req.KeyA, req.KeyB, resp.Value)
		if err != nil {
			log.Println(err)
			resp.Err = err.Error()
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *OortValueStore) Lookup(ctx context.Context, req *valueproto.LookupRequest) (*valueproto.LookupResponse, error) {
	resp := valueproto.LookupResponse{}
	var err error
	resp.TimestampMicro, resp.Length, err = s.vs.Lookup(req.KeyA, req.KeyB)
	if err != nil {
		resp.Err = err.Error()
	}
	return &resp, nil
}

func (s *OortValueStore) StreamLookup(stream valueproto.ValueStore_StreamLookupServer) error {
	var resp valueproto.LookupResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		resp.TimestampMicro, resp.Length, err = s.vs.Lookup(req.KeyA, req.KeyB)
		if err != nil {
			log.Println(err)
			resp.Err = err.Error()
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *OortValueStore) Delete(ctx context.Context, req *valueproto.DeleteRequest) (*valueproto.DeleteResponse, error) {
	resp := valueproto.DeleteResponse{}
	var err error
	resp.TimestampMicro, err = s.vs.Delete(req.KeyA, req.KeyB, req.TimestampMicro)
	if err != nil {
		resp.Err = err.Error()
	}
	return &resp, nil
}

func (s *OortValueStore) StreamDelete(stream valueproto.ValueStore_StreamDeleteServer) error {
	var resp valueproto.DeleteResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		resp.TimestampMicro, err = s.vs.Delete(req.KeyA, req.KeyB, req.TimestampMicro)
		if err != nil {
			log.Println(err)
			resp.Err = err.Error()
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
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
	s.vs.Shutdown()
	s.t.Shutdown()
	s.stopped = true
	s.Unlock()
	log.Println(s.vs.Stats(true))
	log.Println("ValueStore stop complete")
}

func (s *OortValueStore) Stats() []byte {
	return []byte(s.vs.Stats(true).String())
}

func (s *OortValueStore) ListenAndServe() {
	go func(s *OortValueStore) {
		s.grpcStopping = false
		for {
			var err error
			l, err := net.Listen("tcp", s.C.ListenAddr)
			if err != nil {
				log.Fatalln("Unable to bind to address:", err)
			}
			log.Println("ValueStore bound to:", s.C.ListenAddr)
			var opts []grpc.ServerOption
			creds := credentials.NewTLS(s.serverTLSConfig)
			opts = []grpc.ServerOption{grpc.Creds(creds)}
			s.grpc = grpc.NewServer(opts...)
			valueproto.RegisterValueStoreServer(s.grpc, s)
			err = s.grpc.Serve(l)
			if err != nil && !s.grpcStopping {
				log.Println("ValueStore Serve encountered error:", err, "will attempt to restart")
			} else if err != nil && s.grpcStopping {
				log.Println("ValueStore got error but halt is in progress:", err)
				l.Close()
				break
			} else {
				log.Println("ValueStore Serve exited without error, quiting")
				l.Close()
				break
			}
		}
	}(s)
}

func (s *OortValueStore) StopListenAndServe() {
	log.Println("ValueStore shutting down grpc")
	s.grpcStopping = true
	s.grpc.Stop()
}

// Wait isn't implemented yet, need graceful shutdowns in grpc
func (s *OortValueStore) Wait() {}
