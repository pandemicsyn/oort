package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	pb "github.com/pandemicsyn/oort/api/proto"

	"bazil.org/fuse"
	"google.golang.org/grpc"
)

type server struct {
	fs *fs
	wg sync.WaitGroup
}

func newserver(fs *fs) *server {
	s := &server{
		fs: fs,
	}
	return s
}

func (s *server) serve() error {
	defer s.wg.Wait()

	for {
		req, err := s.fs.conn.ReadRequest()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.fs.handle(req)
		}()
	}
	return nil
}

var (
	debug              = flag.Bool("debug", false, "enable debug log messages to stderr")
	serverAddr         = flag.String("host", "127.0.0.1:8443", "The oort api server to connect too")
	serverHostOverride = flag.String("host_override", "localhost", "")
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func debuglog(msg interface{}) {
	fmt.Fprintf(os.Stderr, "%v\n", msg)
}

type rpc struct {
	conn *grpc.ClientConn
	api  pb.ApiClient
}

func newrpc(conn *grpc.ClientConn) *rpc {
	r := &rpc{
		conn: conn,
		api:  pb.NewApiClient(conn),
	}

	return r
}

type NullWriter int

func (NullWriter) Write([]byte) (int, error) { return 0, nil }

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}

	// Setup grpc
	var opts []grpc.DialOption
	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		log.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	// Uncomment the following to diable logs
	//log.SetOutput(new(NullWriter))

	mountpoint := flag.Arg(0)
	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("cfs"),
		fuse.Subtype("cfs"),
		fuse.LocalVolume(),
		fuse.VolumeName("CFS"),
		//fuse.AllowOther(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	rpc := newrpc(conn)
	fs := newfs(c, rpc)
	srv := newserver(fs)

	if err := srv.serve(); err != nil {
		log.Fatal(err)
	}

	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}
