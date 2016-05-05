package oortstore

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/gholt/ring"
	"github.com/gholt/store"
	"github.com/pandemicsyn/ftls"
	"github.com/pandemicsyn/oort/api/proto"
	"github.com/pandemicsyn/oort/api/valueproto"
	"github.com/pandemicsyn/oort/oort"
	"github.com/prometheus/client_golang/prometheus"
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
	msgRing          *ring.TCPMsgRing
	oort             *oort.Server
	Config           *OortValueConfig `toml:"OortValueStoreConfig"` // load config using an explicit/different config header
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
	MutualTLS          bool
	CertFile           string
	KeyFile            string
	CAFile             string
	MetricsAddr        string
	MetricsCollectors  string
}

func NewValueStore(oort *oort.Server) (*OortValueStore, error) {
	s := &OortValueStore{}
	s.Config = &OortValueConfig{}
	s.waitGroup = &sync.WaitGroup{}
	s.oort = oort
	err := s.oort.LoadRingConfig(s)
	if err != nil {
		return s, err
	}
	if s.Config.Debug {
		log.Println("Ring entries:")
		ring := s.oort.Ring()
		for k, _ := range ring.Nodes() {
			log.Println(ring.Nodes()[k].ID(), ring.Nodes()[k].Addresses())
		}
		l := log.New(os.Stdout, "DebugStore ", log.LstdFlags)
		s.ValueStoreConfig.LogDebug = l.Printf
	}
	if s.TCPMsgRingConfig.UseTLS {
		log.Println("TCPMsgRing using TLS")
	}
	if s.TCPMsgRingConfig.AddressIndex == 0 {
		s.TCPMsgRingConfig.AddressIndex = 1
		log.Println("TCPMsgRing using address index 1")
	}
	if s.Config.MutualTLS && s.Config.InsecureSkipVerify {
		return s, fmt.Errorf("Option MutualTLS=true, and InsecureSkipVerify=true conflict")
	}
	s.serverTLSConfig, err = ftls.NewServerTLSConfig(&ftls.Config{
		MutualTLS:          s.Config.MutualTLS,
		InsecureSkipVerify: s.Config.InsecureSkipVerify,
		CertFile:           s.Config.CertFile,
		KeyFile:            s.Config.KeyFile,
		CAFile:             s.Config.CAFile,
	})
	if err != nil {
		return s, err
	}
	s.start()
	s.stopped = false
	return s, nil
}

func (s *OortValueStore) start() {
	s.vs = nil
	runtime.GC()
	log.Println("LocalID appears to be:", s.oort.GetLocalID())
	var err error
	s.msgRing, err = ring.NewTCPMsgRing(&s.TCPMsgRingConfig)
	if err != nil {
		panic(err)
	}
	s.ValueStoreConfig.MsgRing = s.msgRing
	s.msgRing.SetRing(s.oort.Ring())
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
	if err := s.vs.Startup(context.Background()); err != nil {
		panic(err)
	}
	go func(t *ring.TCPMsgRing) {
		t.Listen()
		log.Println("TCPMsgRing Listen() returned, shutdown?")
	}(s.msgRing)
	go func(t *ring.TCPMsgRing) {
		mValues := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "Values",
			Help: "Current number of values stored.",
		})
		mValueBytes := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ValueBytes",
			Help: "Current number of bytes for the values stored.",
		})
		mLookups := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "Lookups",
			Help: "Count of lookup requests executed.",
		})
		mLookupErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "LookupErrors",
			Help: "Count of lookup requests executed resulting in errors.",
		})
		mReads := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "Reads",
			Help: "Count of read requests executed.",
		})
		mReadErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ReadErrors",
			Help: "Count of read requests executed resulting in errors.",
		})
		mWrites := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "Writes",
			Help: "Count of write requests executed.",
		})
		mWriteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "WriteErrors",
			Help: "Count of write requests executed resulting in errors.",
		})
		mWritesOverridden := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "WritesOverridden",
			Help: "Count of write requests that were outdated or repeated.",
		})
		mDeletes := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "Deletes",
			Help: "Count of delete requests executed.",
		})
		mDeleteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "DeleteErrors",
			Help: "Count of delete requests executed resulting in errors.",
		})
		mDeletesOverridden := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "DeletesOverridden",
			Help: "Count of delete requests that were outdated or repeated.",
		})
		mOutBulkSets := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "OutBulkSets",
			Help: "Count of outgoing bulk-set messages in response to incoming pull replication messages.",
		})
		mOutBulkSetValues := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "OutBulkSetValues",
			Help: "Count of values in outgoing bulk-set messages; these bulk-set messages are those in response to incoming pull-replication messages.",
		})
		mOutBulkSetPushes := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "OutBulkSetPushes",
			Help: "Count of outgoing bulk-set messages due to push replication.",
		})
		mOutBulkSetPushValues := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "OutBulkSetPushValues",
			Help: "Count of values in outgoing bulk-set messages; these bulk-set messages are those due to push replication.",
		})
		mInBulkSets := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "InBulkSets",
			Help: "Count of incoming bulk-set messages.",
		})
		mInBulkSetDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "InBulkSetDrops",
			Help: "Count of incoming bulk-set messages dropped due to the local system being overworked at the time.",
		})
		mInBulkSetInvalids := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "InBulkSetInvalids",
			Help: "Count of incoming bulk-set messages that couldn't be parsed.",
		})
		mInBulkSetWrites := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "InBulkSetWrites",
			Help: "Count of writes due to incoming bulk-set messages.",
		})
		mInBulkSetWriteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "InBulkSetWriteErrors",
			Help: "Count of errors returned from writes due to incoming bulk-set messages.",
		})
		mInBulkSetWritesOverridden := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "InBulkSetWritesOverridden",
			Help: "Count of writes from incoming bulk-set messages that result in no change.",
		})
		mOutBulkSetAcks := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "OutBulkSetAcks",
			Help: "Count of outgoing bulk-set-ack messages.",
		})
		mInBulkSetAcks := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "InBulkSetAcks",
			Help: "Count of incoming bulk-set-ack messages.",
		})
		mInBulkSetAckDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "InBulkSetAckDrops",
			Help: "Count of incoming bulk-set-ack messages dropped due to the local system being overworked at the time.",
		})
		mInBulkSetAckInvalids := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "InBulkSetAckInvalids",
			Help: "Count of incoming bulk-set-ack messages that couldn't be parsed.",
		})
		mInBulkSetAckWrites := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "InBulkSetAckWrites",
			Help: "Count of writes (for local removal) due to incoming bulk-set-ack messages.",
		})
		mInBulkSetAckWriteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "InBulkSetAckWriteErrors",
			Help: "Count of errors returned from writes due to incoming bulk-set-ack messages.",
		})
		mInBulkSetAckWritesOverridden := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "InBulkSetAckWritesOverridden",
			Help: "Count of writes from incoming bulk-set-ack messages that result in no change.",
		})
		mOutPullReplications := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "OutPullReplications",
			Help: "Count of outgoing pull-replication messages.",
		})
		mOutPullReplicationSeconds := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "OutPullReplicationSeconds",
			Help: "How long the last out pull replication pass took.",
		})
		mInPullReplications := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "InPullReplications",
			Help: "Count of incoming pull-replication messages.",
		})
		mInPullReplicationDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "InPullReplicationDrops",
			Help: "Count of incoming pull-replication messages droppped due to the local system being overworked at the time.",
		})
		mInPullReplicationInvalids := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "InPullReplicationInvalids",
			Help: "Count of incoming pull-replication messages that couldn't be parsed.",
		})
		mExpiredDeletions := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ExpiredDeletions",
			Help: "Count of recent deletes that have become old enough to be completely discarded.",
		})
		mCompactions := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "Compactions",
			Help: "Count of disk file sets compacted due to their contents exceeding a staleness threshold. For example, this happens when enough of the values have been overwritten or deleted in more recent operations.",
		})
		mSmallFileCompactions := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "SmallFileCompactions",
			Help: "Count of disk file sets compacted due to the entire file size being too small. For example, this may happen when the store is shutdown and restarted.",
		})
		prometheus.Register(mValues)
		prometheus.Register(mValueBytes)
		prometheus.Register(mLookups)
		prometheus.Register(mLookupErrors)
		prometheus.Register(mReads)
		prometheus.Register(mReadErrors)
		prometheus.Register(mWrites)
		prometheus.Register(mWriteErrors)
		prometheus.Register(mWritesOverridden)
		prometheus.Register(mDeletes)
		prometheus.Register(mDeleteErrors)
		prometheus.Register(mDeletesOverridden)
		prometheus.Register(mOutBulkSets)
		prometheus.Register(mOutBulkSetValues)
		prometheus.Register(mOutBulkSetPushes)
		prometheus.Register(mOutBulkSetPushValues)
		prometheus.Register(mInBulkSets)
		prometheus.Register(mInBulkSetDrops)
		prometheus.Register(mInBulkSetInvalids)
		prometheus.Register(mInBulkSetWrites)
		prometheus.Register(mInBulkSetWriteErrors)
		prometheus.Register(mInBulkSetWritesOverridden)
		prometheus.Register(mOutBulkSetAcks)
		prometheus.Register(mInBulkSetAcks)
		prometheus.Register(mInBulkSetAckDrops)
		prometheus.Register(mInBulkSetAckInvalids)
		prometheus.Register(mInBulkSetAckWrites)
		prometheus.Register(mInBulkSetAckWriteErrors)
		prometheus.Register(mInBulkSetAckWritesOverridden)
		prometheus.Register(mOutPullReplications)
		prometheus.Register(mOutPullReplicationSeconds)
		prometheus.Register(mInPullReplications)
		prometheus.Register(mInPullReplicationDrops)
		prometheus.Register(mInPullReplicationInvalids)
		prometheus.Register(mExpiredDeletions)
		prometheus.Register(mCompactions)
		prometheus.Register(mSmallFileCompactions)
		tcpMsgRingStats := t.Stats(false)
		for !tcpMsgRingStats.Shutdown {
			time.Sleep(time.Minute)
			tcpMsgRingStats = t.Stats(false)
			log.Printf("%v\n", tcpMsgRingStats)
			stats, err := s.vs.Stats(context.Background(), false)
			if err != nil {
				log.Printf("stats error: %s\n", err)
			} else if s, ok := stats.(*store.ValueStoreStats); ok {
				mValues.Set(float64(s.Values))
				mValueBytes.Set(float64(s.ValueBytes))
				mLookups.Add(float64(s.Lookups))
				mLookupErrors.Add(float64(s.LookupErrors))
				mReads.Add(float64(s.Reads))
				mReadErrors.Add(float64(s.ReadErrors))
				mWrites.Add(float64(s.Writes))
				mWriteErrors.Add(float64(s.WriteErrors))
				mWritesOverridden.Add(float64(s.WritesOverridden))
				mDeletes.Add(float64(s.Deletes))
				mDeleteErrors.Add(float64(s.DeleteErrors))
				mDeletesOverridden.Add(float64(s.DeletesOverridden))
				mOutBulkSets.Add(float64(s.OutBulkSets))
				mOutBulkSetValues.Add(float64(s.OutBulkSetValues))
				mOutBulkSetPushes.Add(float64(s.OutBulkSetPushes))
				mOutBulkSetPushValues.Add(float64(s.OutBulkSetPushValues))
				mInBulkSets.Add(float64(s.InBulkSets))
				mInBulkSetDrops.Add(float64(s.InBulkSetDrops))
				mInBulkSetInvalids.Add(float64(s.InBulkSetInvalids))
				mInBulkSetWrites.Add(float64(s.InBulkSetWrites))
				mInBulkSetWriteErrors.Add(float64(s.InBulkSetWriteErrors))
				mInBulkSetWritesOverridden.Add(float64(s.InBulkSetWritesOverridden))
				mOutBulkSetAcks.Add(float64(s.OutBulkSetAcks))
				mInBulkSetAcks.Add(float64(s.InBulkSetAcks))
				mInBulkSetAckDrops.Add(float64(s.InBulkSetAckDrops))
				mInBulkSetAckInvalids.Add(float64(s.InBulkSetAckInvalids))
				mInBulkSetAckWrites.Add(float64(s.InBulkSetAckWrites))
				mInBulkSetAckWriteErrors.Add(float64(s.InBulkSetAckWriteErrors))
				mInBulkSetAckWritesOverridden.Add(float64(s.InBulkSetAckWritesOverridden))
				mOutPullReplications.Add(float64(s.OutPullReplications))
				mOutPullReplicationSeconds.Set(float64(s.OutPullReplicationNanoseconds) / 1000000000)
				mInPullReplications.Add(float64(s.InPullReplications))
				mInPullReplicationDrops.Add(float64(s.InPullReplicationDrops))
				mInPullReplicationInvalids.Add(float64(s.InPullReplicationInvalids))
				mExpiredDeletions.Add(float64(s.ExpiredDeletions))
				mCompactions.Add(float64(s.Compactions))
				mSmallFileCompactions.Add(float64(s.SmallFileCompactions))
			} else {
				log.Printf("%s\n", stats)
			}
		}
		prometheus.Unregister(mValues)
		prometheus.Unregister(mValueBytes)
		prometheus.Unregister(mLookups)
		prometheus.Unregister(mLookupErrors)
		prometheus.Unregister(mReads)
		prometheus.Unregister(mReadErrors)
		prometheus.Unregister(mWrites)
		prometheus.Unregister(mWriteErrors)
		prometheus.Unregister(mWritesOverridden)
		prometheus.Unregister(mDeletes)
		prometheus.Unregister(mDeleteErrors)
		prometheus.Unregister(mDeletesOverridden)
		prometheus.Unregister(mOutBulkSets)
		prometheus.Unregister(mOutBulkSetValues)
		prometheus.Unregister(mOutBulkSetPushes)
		prometheus.Unregister(mOutBulkSetPushValues)
		prometheus.Unregister(mInBulkSets)
		prometheus.Unregister(mInBulkSetDrops)
		prometheus.Unregister(mInBulkSetInvalids)
		prometheus.Unregister(mInBulkSetWrites)
		prometheus.Unregister(mInBulkSetWriteErrors)
		prometheus.Unregister(mInBulkSetWritesOverridden)
		prometheus.Unregister(mOutBulkSetAcks)
		prometheus.Unregister(mInBulkSetAcks)
		prometheus.Unregister(mInBulkSetAckDrops)
		prometheus.Unregister(mInBulkSetAckInvalids)
		prometheus.Unregister(mInBulkSetAckWrites)
		prometheus.Unregister(mInBulkSetAckWriteErrors)
		prometheus.Unregister(mInBulkSetAckWritesOverridden)
		prometheus.Unregister(mOutPullReplications)
		prometheus.Unregister(mOutPullReplicationSeconds)
		prometheus.Unregister(mInPullReplications)
		prometheus.Unregister(mInPullReplicationDrops)
		prometheus.Unregister(mInPullReplicationInvalids)
		prometheus.Unregister(mExpiredDeletions)
		prometheus.Unregister(mCompactions)
		prometheus.Unregister(mSmallFileCompactions)
	}(s.msgRing)
}

func (s *OortValueStore) UpdateRing(ring ring.Ring) {
	s.Lock()
	s.msgRing.SetRing(ring)
	s.Unlock()
	log.Println("Oortstore updated tcp msg ring.")
}

func (s *OortValueStore) Write(ctx context.Context, req *valueproto.WriteRequest) (*valueproto.WriteResponse, error) {
	resp := valueproto.WriteResponse{Rpcid: req.Rpcid}
	var err error
	resp.TimestampMicro, err = s.vs.Write(ctx, req.KeyA, req.KeyB, req.TimestampMicro, req.Value)
	if err != nil {
		resp.Err = proto.TranslateError(err)
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
		resp.Rpcid = req.Rpcid
		resp.TimestampMicro, err = s.vs.Write(stream.Context(), req.KeyA, req.KeyB, req.TimestampMicro, req.Value)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *OortValueStore) Read(ctx context.Context, req *valueproto.ReadRequest) (*valueproto.ReadResponse, error) {
	resp := valueproto.ReadResponse{Rpcid: req.Rpcid}
	var err error
	resp.TimestampMicro, resp.Value, err = s.vs.Read(ctx, req.KeyA, req.KeyB, resp.Value)
	if err != nil {
		resp.Err = proto.TranslateError(err)
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
		resp.Rpcid = req.Rpcid
		resp.TimestampMicro, resp.Value, err = s.vs.Read(stream.Context(), req.KeyA, req.KeyB, resp.Value)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *OortValueStore) Lookup(ctx context.Context, req *valueproto.LookupRequest) (*valueproto.LookupResponse, error) {
	resp := valueproto.LookupResponse{Rpcid: req.Rpcid}
	var err error
	resp.TimestampMicro, resp.Length, err = s.vs.Lookup(ctx, req.KeyA, req.KeyB)
	if err != nil {
		resp.Err = proto.TranslateError(err)
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
		resp.Rpcid = req.Rpcid
		resp.TimestampMicro, resp.Length, err = s.vs.Lookup(stream.Context(), req.KeyA, req.KeyB)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *OortValueStore) Delete(ctx context.Context, req *valueproto.DeleteRequest) (*valueproto.DeleteResponse, error) {
	resp := valueproto.DeleteResponse{Rpcid: req.Rpcid}
	var err error
	resp.TimestampMicro, err = s.vs.Delete(ctx, req.KeyA, req.KeyB, req.TimestampMicro)
	if err != nil {
		resp.Err = proto.TranslateError(err)
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
		resp.Rpcid = req.Rpcid
		resp.TimestampMicro, err = s.vs.Delete(stream.Context(), req.KeyA, req.KeyB, req.TimestampMicro)
		if err != nil {
			resp.Err = proto.TranslateError(err)
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
	log.Println(s.vs.Stats(context.Background(), true))
	log.Println("ValueStore start complete")
}

func (s *OortValueStore) Stop() {
	s.Lock()
	if s.stopped {
		s.Unlock()
		return
	}
	s.vs.Shutdown(context.Background())
	s.msgRing.Shutdown()
	s.stopped = true
	s.Unlock()
	log.Println(s.vs.Stats(context.Background(), true))
	log.Println("ValueStore stop complete")
}

func (s *OortValueStore) Stats() []byte {
	stats, err := s.vs.Stats(context.Background(), true)
	if err != nil {
		return nil
	}
	return []byte(stats.String())
}

func (s *OortValueStore) ListenAndServe() {
	go func(s *OortValueStore) {
		s.Lock()
		s.grpcStopping = false
		s.Unlock()
		for {
			s.Lock()
			var err error
			listenAddr := s.oort.GetListenAddr()
			if listenAddr == "" {
				log.Fatalln("No listen address specified in ring at address2")
			}
			l, err := net.Listen("tcp", listenAddr)
			if err != nil {
				log.Fatalln("Unable to bind to address:", err)
			}
			log.Println("ValueStore bound to:", listenAddr)
			var opts []grpc.ServerOption
			creds := credentials.NewTLS(s.serverTLSConfig)
			opts = []grpc.ServerOption{grpc.Creds(creds)}
			srvr := grpc.NewServer(opts...)
			s.grpc = srvr
			valueproto.RegisterValueStoreServer(s.grpc, s)
			s.Unlock()
			err = srvr.Serve(l)
			s.Lock()
			if err != nil && !s.grpcStopping {
				log.Println("ValueStore Serve encountered error:", err, "will attempt to restart")
			} else if err != nil && s.grpcStopping {
				log.Println("ValueStore got error but halt is in progress:", err)
				l.Close()
				s.Unlock()
				break
			} else {
				log.Println("ValueStore Serve exited without error, quiting")
				l.Close()
				s.Unlock()
				break
			}
			s.Unlock()
		}
	}(s)
}

func (s *OortValueStore) StopListenAndServe() {
	s.Lock()
	log.Println("ValueStore shutting down grpc")
	s.grpcStopping = true
	s.grpc.Stop()
	s.Unlock()
}

// Wait isn't implemented yet, need graceful shutdowns in grpc
func (s *OortValueStore) Wait() {}
