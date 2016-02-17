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
	"github.com/pandemicsyn/oort/api/groupproto"
	"github.com/pandemicsyn/oort/oort"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type OortGroupStore struct {
	sync.RWMutex
	waitGroup        *sync.WaitGroup
	vs               store.GroupStore
	grpc             *grpc.Server
	grpcStopping     bool
	t                *ring.TCPMsgRing
	o                *oort.Server
	C                *OortGroupConfig `toml:"OortGroupStoreConfig"` // load config using an explicit/different config header
	stopped          bool
	GroupStoreConfig store.GroupStoreConfig
	TCPMsgRingConfig ring.TCPMsgRingConfig
	serverTLSConfig  *tls.Config
}

type OortGroupConfig struct {
	Debug              bool
	Profile            bool
	ListenAddr         string `toml:"ListenAddress"` //another example
	InsecureSkipVerify bool
	CertFile           string
	KeyFile            string
}

func NewGroupStore(oort *oort.Server) (*OortGroupStore, error) {
	s := &OortGroupStore{}
	s.C = &OortGroupConfig{}
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
		s.GroupStoreConfig.LogDebug = l.Printf
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

func (s *OortGroupStore) start() {
	s.vs = nil
	runtime.GC()
	log.Println("LocalID appears to be:", s.o.GetLocalID())
	s.t = ring.NewTCPMsgRing(&s.TCPMsgRingConfig)
	s.GroupStoreConfig.MsgRing = s.t
	s.t.SetRing(s.o.Ring())
	var restartChan chan error
	s.vs, restartChan = store.NewGroupStore(&s.GroupStoreConfig)
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
			stats, err := s.vs.Stats(false)
			if err != nil {
				log.Printf("stats error: %s\n", err)
			} else {
				log.Printf("%s\n", stats)
			}
		}
	}(s.t)
}

func (s *OortGroupStore) UpdateRing(ring ring.Ring) {
	s.Lock()
	s.t.SetRing(ring)
	s.Unlock()
	log.Println("Oortstore updated tcp msg ring.")
}

func (s *OortGroupStore) Write(ctx context.Context, req *groupproto.WriteRequest) (*groupproto.WriteResponse, error) {
	resp := groupproto.WriteResponse{}
	var err error
	resp.TimestampMicro, err = s.vs.Write(req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, req.TimestampMicro, req.Value)
	if err != nil {
		log.Println(err)
		resp.Err = err.Error()
	}
	return &resp, nil
}

func (s *OortGroupStore) StreamWrite(stream groupproto.GroupStore_StreamWriteServer) error {
	var resp groupproto.WriteResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		resp.TimestampMicro, err = s.vs.Write(req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, req.TimestampMicro, req.Value)
		if err != nil {
			log.Println(err)
			resp.Err = err.Error()
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *OortGroupStore) Read(ctx context.Context, req *groupproto.ReadRequest) (*groupproto.ReadResponse, error) {
	resp := groupproto.ReadResponse{}
	var err error
	resp.TimestampMicro, resp.Value, err = s.vs.Read(req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, resp.Value)
	if err != nil {
		resp.Err = err.Error()
	}
	return &resp, nil
}

func (s *OortGroupStore) StreamRead(stream groupproto.GroupStore_StreamReadServer) error {
	var resp groupproto.ReadResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		resp.TimestampMicro, resp.Value, err = s.vs.Read(req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, resp.Value)
		if err != nil {
			log.Println(err)
			resp.Err = err.Error()
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *OortGroupStore) Lookup(ctx context.Context, req *groupproto.LookupRequest) (*groupproto.LookupResponse, error) {
	resp := groupproto.LookupResponse{}
	var err error
	resp.TimestampMicro, resp.Length, err = s.vs.Lookup(req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB)
	if err != nil {
		resp.Err = err.Error()
	}
	return &resp, nil
}

func (s *OortGroupStore) StreamLookup(stream groupproto.GroupStore_StreamLookupServer) error {
	var resp groupproto.LookupResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		resp.TimestampMicro, resp.Length, err = s.vs.Lookup(req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB)
		if err != nil {
			log.Println(err)
			resp.Err = err.Error()
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *OortGroupStore) LookupGroup(ctx context.Context, req *groupproto.LookupGroupRequest) (*groupproto.LookupGroupResponse, error) {
	resp := &groupproto.LookupGroupResponse{}
	items, err := s.vs.LookupGroup(req.KeyA, req.KeyB)
	if err != nil {
		resp.Err = err.Error()
	} else {
		for _, v := range items {
			g := groupproto.LookupGroupItem{}
			g.Length = v.Length
			g.ChildKeyA = v.ChildKeyA
			g.ChildKeyB = v.ChildKeyB
			g.TimestampMicro = v.TimestampMicro
			resp.Items = append(resp.Items, &g)
		}
	}
	return resp, nil
}

func (s *OortGroupStore) StreamLookupGroup(stream groupproto.GroupStore_StreamLookupGroupServer) error {
	var resp groupproto.LookupGroupResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		items, err := s.vs.LookupGroup(req.KeyA, req.KeyB)
		if err != nil {
			resp.Err = err.Error()
		} else {
			for _, v := range items {
				g := groupproto.LookupGroupItem{}
				g.Length = v.Length
				g.ChildKeyA = v.ChildKeyA
				g.ChildKeyB = v.ChildKeyB
				g.TimestampMicro = v.TimestampMicro
				resp.Items = append(resp.Items, &g)
			}
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *OortGroupStore) ReadGroup(ctx context.Context, req *groupproto.ReadGroupRequest) (*groupproto.ReadGroupResponse, error) {
	resp := groupproto.ReadGroupResponse{}
	lgis, err := s.vs.LookupGroup(req.KeyA, req.KeyB)
	if err != nil {
		resp.Err = err.Error()
	} else {
		resp.Items = make([]*groupproto.ReadGroupItem, len(lgis))
		itemCount := 0
		var err error
		for _, lgi := range lgis {
			g := groupproto.ReadGroupItem{}
			g.TimestampMicro, g.Value, err = s.vs.Read(req.KeyA, req.KeyB, lgi.ChildKeyA, lgi.ChildKeyB, nil)
			if err != nil {
				continue
			}
			g.ChildKeyA = lgi.ChildKeyA
			g.ChildKeyB = lgi.ChildKeyB
			itemCount++
		}
		resp.Items = resp.Items[:itemCount]
	}
	return &resp, nil
}

func (s *OortGroupStore) StreamReadGroup(stream groupproto.GroupStore_StreamReadGroupServer) error {
	var resp groupproto.ReadGroupResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		lgis, err := s.vs.LookupGroup(req.KeyA, req.KeyB)
		if err != nil {
			resp.Err = err.Error()
		} else {
			resp.Items = make([]*groupproto.ReadGroupItem, len(lgis))
			itemCount := 0
			for _, lgi := range lgis {
				g := groupproto.ReadGroupItem{}
				g.TimestampMicro, g.Value, err = s.vs.Read(req.KeyA, req.KeyB, lgi.ChildKeyA, lgi.ChildKeyB, nil)
				if err != nil {
					continue
				}
				g.ChildKeyA = lgi.ChildKeyA
				g.ChildKeyB = lgi.ChildKeyB
				itemCount++
			}
			resp.Items = resp.Items[:itemCount]
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *OortGroupStore) Delete(ctx context.Context, req *groupproto.DeleteRequest) (*groupproto.DeleteResponse, error) {
	resp := groupproto.DeleteResponse{}
	var err error
	resp.TimestampMicro, err = s.vs.Delete(req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, req.TimestampMicro)
	if err != nil {
		resp.Err = err.Error()
	}
	return &resp, nil
}

func (s *OortGroupStore) StreamDelete(stream groupproto.GroupStore_StreamDeleteServer) error {
	var resp groupproto.DeleteResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		resp.TimestampMicro, err = s.vs.Delete(req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, req.TimestampMicro)
		if err != nil {
			log.Println(err)
			resp.Err = err.Error()
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
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
	s.vs.Shutdown()
	s.t.Shutdown()
	s.stopped = true
	s.Unlock()
	log.Println(s.vs.Stats(true))
	log.Println("GroupStore stop complete")
}

func (s *OortGroupStore) Stats() []byte {
	stats, err := s.vs.Stats(true)
	if err != nil {
		log.Println(err)
		return nil
	}
	return []byte(stats.String())
}

func (s *OortGroupStore) ListenAndServe() {
	go func(s *OortGroupStore) {
		s.grpcStopping = false
		for {
			var err error
			l, err := net.Listen("tcp", s.C.ListenAddr)
			if err != nil {
				log.Fatalln("Unable to bind to address:", err)
			}
			log.Println("GroupStore bound to:", s.C.ListenAddr)
			var opts []grpc.ServerOption
			creds := credentials.NewTLS(s.serverTLSConfig)
			opts = []grpc.ServerOption{grpc.Creds(creds)}
			s.grpc = grpc.NewServer(opts...)
			groupproto.RegisterGroupStoreServer(s.grpc, s)
			err = s.grpc.Serve(l)
			if err != nil && !s.grpcStopping {
				log.Println("GroupStore Serve encountered error:", err, "will attempt to restart")
			} else if err != nil && s.grpcStopping {
				log.Println("GroupStore got error but halt is in progress:", err)
				l.Close()
				break
			} else {
				log.Println("GroupStore Serve exited without error, quiting")
				l.Close()
				break
			}
		}
	}(s)
}

func (s *OortGroupStore) StopListenAndServe() {
	log.Println("GroupStore shutting down grpc")
	s.grpcStopping = true
	s.grpc.Stop()
}

// Wait isn't implemented yet, need graceful shutdowns in grpc
func (s *OortGroupStore) Wait() {}
