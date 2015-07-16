package main

import (
	"flag"
	"fmt"

	pb "github.com/pandemicsyn/ort/ort-api/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"

	"net"
	"sync"
)

var (
	tls      = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile = flag.String("cert_file", "server.crt", "The TLS cert file")
	keyFile  = flag.String("key_file", "server.key", "The TLS key file")
	port     = flag.Int("port", 8443, "The server port")
)

func FatalIf(err error, msg string) {
	if err != nil {
		grpclog.Fatalf("%s: %v", msg, err)
	}
}

type dirServer struct {
	sync.RWMutex
	test string
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
	test string
}

func (s *fileServer) GetAttr(ctx context.Context, f *pb.FileRequest) (*pb.FileAttr, error) {
	return &pb.FileAttr{}, nil
}

func (s *fileServer) SetAttr(ctx context.Context, f *pb.FileAttr) (*pb.FileAttr, error) {
	return &pb.FileAttr{}, nil
}

func (s *fileServer) Read(ctx context.Context, f *pb.FileRequest) (*pb.File, error) {
	return &pb.File{}, nil
}

func (s *fileServer) Write(ctx context.Context, f *pb.File) (*pb.WriteResponse, error) {
	return &pb.WriteResponse{}, nil
}

func newDirServer() *dirServer {
	s := new(dirServer)
	s.test = "wat"
	return s
}

func newFileServer() *fileServer {
	s := new(fileServer)
	s.test = "wat"
	return s
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
	pb.RegisterFileInterfaceServer(s, newFileServer())
	pb.RegisterDirInterfaceServer(s, newDirServer())
	grpclog.Printf("Starting up on %d...\n", *port)
	s.Serve(l)
}
