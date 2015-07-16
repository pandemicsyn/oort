package main

import (
	pb "github.com/pandemicsyn/ort/api/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	"flag"
	"time"
)

var (
	serverAddr         = flag.String("host", "127.0.0.1:8443", "The server address in the format of host:port")
	serverHostOverride = flag.String("host_override", "localhost", "The server name use to verify the hostname returned by TLS handshake")
)

func printAttr(client pb.FileApiClient, fr *pb.FileRequest) {
	grpclog.Printf("Getting attrs for %s", fr.Fpath)

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	attrs, err := client.GetAttr(ctx, fr)
	if err != nil {
		grpclog.Fatalf("%v.GetAttr(_) = _, %v: ", client, err)
	}
	grpclog.Println(attrs)
}

func main() {
	flag.Parse()
	var opts []grpc.DialOption

	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		grpclog.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewFileApiClient(conn)

	printAttr(client, &pb.FileRequest{Fpath: "/d1/d2/test.file"})
}
