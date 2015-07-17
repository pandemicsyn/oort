package main

import (
	"flag"
	"fmt"

	"github.com/garyburd/redigo/redis"
	"golang.org/x/net/context"
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

type dirServer struct {
	sync.RWMutex
	rpool *redis.Pool
}

func (s *dirServer) Create(ctx context.Context, f *pb.DirEnt) (*pb.WriteResponse, error) {
	return &pb.WriteResponse{}, nil
}

func (s *dirServer) Lookup(ctx context.Context, f *pb.DirRequest) (*pb.DirEnt, error) {
	return &pb.DirEnt{}, nil
}

func (s *dirServer) ReadDirAll(ctx context.Context, f *pb.DirRequest) (*pb.DirEntries, error) {
	return &pb.DirEntries{}, nil
}

func (s *dirServer) Remove(ctx context.Context, f *pb.DirEnt) (*pb.WriteResponse, error) {
	return &pb.WriteResponse{}, nil
}

type fileServer struct {
	sync.RWMutex
	rpool *redis.Pool
}

func (s *fileServer) GetAttr(ctx context.Context, r *pb.FileRequest) (*pb.FileAttr, error) {
	f := &pb.FileAttr{
		Parent: "wat",
		Name:   r.Fpath,
		Mode:   "0777",
		Size:   42,
		Mtime:  42,
	}
	return f, nil
}

func (s *fileServer) SetAttr(ctx context.Context, r *pb.FileAttr) (*pb.FileAttr, error) {
	f := &pb.FileAttr{
		Parent: "wat",
		Name:   r.Name,
		Mode:   r.Mode,
		Size:   42,
		Mtime:  r.Mtime,
	}
	return f, nil
}

func (s *fileServer) Read(ctx context.Context, r *pb.FileRequest) (*pb.File, error) {
	var err error
	rc := s.rpool.Get()
	defer rc.Close()
	data, err := redis.Bytes(rc.Do("GET", r.Fpath))
	if err != nil {
		return &pb.File{}, err
	}
	f := &pb.File{Name: r.Fpath, Payload: data}
	return f, nil
}

func (s *fileServer) Write(ctx context.Context, r *pb.File) (*pb.WriteResponse, error) {
	rc := s.rpool.Get()
	defer rc.Close()
	_, err := rc.Do("SET", r.Name, r.Payload)
	if err != nil {
		return &pb.WriteResponse{Status: 1}, err
	}
	rc.Close()
	return &pb.WriteResponse{Status: 0}, nil
}

func newDirServer() *dirServer {
	s := new(dirServer)
	s.rpool = newRedisPool(*ortHost)
	return s
}

func newFileServer() *fileServer {
	s := new(fileServer)
	s.rpool = newRedisPool(*ortHost)
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
	s := grpc.NewServer(opts...)
	pb.RegisterFileApiServer(s, newFileServer())
	pb.RegisterDirApiServer(s, newDirServer())
	grpclog.Printf("Starting up on %d...\n", *port)
	s.Serve(l)
}
