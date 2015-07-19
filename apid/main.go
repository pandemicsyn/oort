package main

import (
	"flag"
	"fmt"

	"github.com/garyburd/redigo/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"

	pb "github.com/pandemicsyn/ort/api/proto"

	"net"
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

func newDirServer() *dirServer {
	s := new(dirServer)
	s.rpool = newRedisPool(*ortHost)
	s.dirs = make(map[string]Dir)
	return s
}

func newFileServer() *fileServer {
	s := new(fileServer)
	s.rpool = newRedisPool(*ortHost)
	s.files = make(map[string]*pb.FileAttr)
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
