package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/garyburd/redigo/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"

	pb "github.com/pandemicsyn/ort/api/proto"

	"net"
	"sync"
	"time"
)

var (
	tls      = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile = flag.String("cert_file", "server.crt", "The TLS cert file")
	keyFile  = flag.String("key_file", "server.key", "The TLS key file")
	port     = flag.Int("port", 8443, "The server port")
	ortHost  = flag.String("orthost", "127.0.0.1:6379", "host:port to use when connecting to ort")
)

func FatalIf(err error, msg string) {
	if err != nil {
		grpclog.Fatalf("%s: %v", msg, err)
	}
}

func genUUID() string {
	f, _ := os.Open("/dev/urandom")
	b := make([]byte, 16)
	f.Read(b)
	f.Close()
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func newDirServer(fs *InMemFS) *dirServer {
	s := new(dirServer)
	s.rpool = newRedisPool(*ortHost)
	s.fs = fs
	return s
}

func newFileServer(fs *InMemFS) *fileServer {
	s := new(fileServer)
	s.rpool = newRedisPool(*ortHost)
	s.fs = fs
	s.files = make(map[uint64]*pb.Attr)
	return s
}

func newRedisPool(server string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},
	}
}

type InMemFS struct {
	sync.RWMutex
	nodes map[uint64]*Entry
}

type Entry struct {
	path  string
	isdir bool
	sync.RWMutex
	attr      *pb.Attr
	parent    uint64
	UUIDNode  int64
	entries   map[string]uint64
	ientries  map[uint64]string
	nodeCount uint64
}

func main() {
	flag.Parse()

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	FatalIf(err, "Failed to bind to port")

	var opts []grpc.ServerOption
	if *tls {
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		FatalIf(err, "Couldn't load cert from file")
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	fs := &InMemFS{nodes: make(map[uint64]*Entry)}
	s := grpc.NewServer(opts...)
	pb.RegisterFileApiServer(s, newFileServer(fs))
	pb.RegisterDirApiServer(s, newDirServer(fs))
	grpclog.Printf("Starting up on %d...\n", *port)
	s.Serve(l)
}
