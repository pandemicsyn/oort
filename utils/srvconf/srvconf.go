package srvconf

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"time"

	pb "github.com/pandemicsyn/syndicate/api/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	ErrSRVLookupFailed = errors.New("srv lookup failed")
)

// lookup returned records are sorted by priority and randomized by weight within a priority.
func lookup(service string) ([]*net.SRV, error) {
	_, addrs, err := net.LookupSRV("", "", service)
	if err != nil {
		log.Println("srv:", service)
		log.Println(err)
		return nil, ErrSRVLookupFailed
	}
	return addrs, nil
}

type SRVLoader struct {
	Record       string
	SyndicateURL string
}

func (s *SRVLoader) getConfig() (*pb.NodeConfig, error) {
	nconfig := &pb.NodeConfig{}
	var opts []grpc.DialOption
	var creds credentials.TransportAuthenticator
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	conn, err := grpc.Dial(s.SyndicateURL, opts...)
	if err != nil {
		return nconfig, fmt.Errorf("Failed to dial ring server for config: %s", err)
	}
	defer conn.Close()

	client := pb.NewRingMgrClient(conn)

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	rr := &pb.RegisterRequest{}
	rr.Hostname, _ = os.Hostname()
	addrs, _ := net.InterfaceAddrs()
	for k, _ := range addrs {
		rr.Addrs = append(rr.Addrs, addrs[k].String())
	}
	rr.Disks = 2
	rr.Cores = int32(runtime.NumCPU())
	rr.Hardwareid = "something"
	rr.Tiers = []string{rr.Hostname}

	nconfig, err = client.RegisterNode(ctx, rr)
	return nconfig, err
}

func (s *SRVLoader) Load() (nodeconfig *pb.NodeConfig, err error) {
	if s.SyndicateURL == "" {
		serviceAddrs, err := lookup(s.Record)
		if err != nil {
			return &pb.NodeConfig{}, err
		}
		s.SyndicateURL = fmt.Sprintf("%s:%d", serviceAddrs[0].Target, serviceAddrs[0].Port)
	}
	nodeconfig, err = s.getConfig()
	if err != nil {
		if err == ErrSRVLookupFailed {
			return nodeconfig, err
		}
		return nodeconfig, err
	}
	return nodeconfig, nil
}
