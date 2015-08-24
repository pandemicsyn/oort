package main

import (
	"flag"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/gholt/ring"
	pb "github.com/pandemicsyn/ort/ortring/api/proto"

	"log"
	"net"
	"strings"
)

var (
	tls             = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile        = flag.String("cert_file", "server.crt", "The TLS cert file")
	keyFile         = flag.String("key_file", "server.key", "The TLS key file")
	builderFile     = flag.String("builder_file", "/etc/ort/ort.builder", "path to builder file")
	ringFile        = flag.String("ring_file", "/etc/ort/ort.ring", "path to ring file")
	port            = flag.Int("port", 8443, "The server port")
	ringSlavesArg   = flag.String("ring_slaves", "", "comma sep list of ring slaves")
	master          = flag.Bool("master", false, "Whether or not this node is a master")
	_SYN_NET_FILTER = []string{"10.0.0.0/8", "192.168.0.0/16"} //need to pull from conf
)

// FatalIf is just a lazy log/panic on error func
func FatalIf(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func newRingMgrServer(slaves []*RingSlave) (*ringmgr, error) {
	var err error
	s := new(ringmgr)
	log.Println("using", *builderFile)
	_, s.b, err = ring.RingOrBuilder(*builderFile)
	FatalIf(err, "Builder file load")
	s.r, _, err = ring.RingOrBuilder(*ringFile)
	FatalIf(err, "Ring file load")
	s.version = s.r.Version()
	log.Println("Ring version is:", s.version)
	s.rb, s.bb, err = s.loadRingBuilderBytes(s.version)
	FatalIf(err, "Attempting to load ring/builder bytes")

	for _, v := range _SYN_NET_FILTER {
		_, n, err := net.ParseCIDR(v)
		if err != nil {
			FatalIf(err, "Invalid network range provided")
		}
		s.netlimits = append(s.netlimits, n)
	}

	s.slaves = slaves

	if len(s.slaves) == 0 {
		log.Println("!! Running without slaves, have no one to register !!")
		return s, nil
	}

	failcount := 0
	for _, slave := range s.slaves {
		if err = s.RegisterSlave(slave); err != nil {
			log.Println("Got error:", err)
			failcount++
		}
	}
	if failcount > (len(s.slaves) / 2) {
		log.Fatalln("More than half of the ring slaves failed to respond. Exiting.")
	}
	return s, nil
}

func newRingDistServer() *ringslave {
	s := new(ringslave)
	return s
}

func parseSlaveAddrs(slaveArg string) []*RingSlave {
	if slaveArg == "" {
		return make([]*RingSlave, 0)
	}
	slaveAddrs := strings.Split(slaveArg, ",")
	slaves := make([]*RingSlave, len(slaveAddrs))
	for i, v := range slaveAddrs {
		slaves[i] = &RingSlave{
			status: false,
			addr:   v,
		}
	}
	return slaves
}

func main() {
	flag.Parse()
	if *master {
		l, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
		FatalIf(err, "Failed to bind to port")
		var opts []grpc.ServerOption
		if *tls {
			creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
			FatalIf(err, "Couldn't load cert from file")
			opts = []grpc.ServerOption{grpc.Creds(creds)}
		}
		s := grpc.NewServer(opts...)

		r, err := newRingMgrServer(parseSlaveAddrs(*ringSlavesArg))
		FatalIf(err, "Couldn't prep ring mgr server")
		pb.RegisterRingMgrServer(s, r)
		log.Printf("Master starting up on %d...\n", *port)
		s.Serve(l)
	} else {
		l, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
		FatalIf(err, "Failed to bind to port")
		var opts []grpc.ServerOption
		if *tls {
			creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
			FatalIf(err, "Couldn't load cert from file")
			opts = []grpc.ServerOption{grpc.Creds(creds)}
		}
		s := grpc.NewServer(opts...)

		pb.RegisterRingDistServer(s, newRingDistServer())
		log.Printf("Starting ring slave up on %d...\n", *port)
		s.Serve(l)
	}
}
