package srvconf

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"time"

	pb "github.com/pandemicsyn/ort/ortring/api/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
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
	Record string
}

func (s *SRVLoader) getConfig(service string) (*pb.NodeConfig, error) {
	nconfig := &pb.NodeConfig{}
	serviceAddrs, err := lookup(service)
	if err != nil {
		return nconfig, err
	}

	var opts []grpc.DialOption
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", serviceAddrs[0].Target, serviceAddrs[0].Port), opts...)
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

	nconfig, err = client.RegisterNode(ctx, rr)
	return nconfig, err
}

func (s *SRVLoader) Load() (nodeconfig *pb.NodeConfig, err error) {
	nodeconfig, err = s.getConfig(s.Record)
	if err != nil {
		if err == ErrSRVLookupFailed {
			log.Println("SRV Lookup failed, assuming srv opts not being used")
			return nodeconfig, nil
		}
		return nodeconfig, err
	}
	return nodeconfig, nil
}
