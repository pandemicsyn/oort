package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gholt/brimtext"
	"github.com/gholt/ring"
	pb "github.com/pandemicsyn/ort/ortring/api/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	_SYN_REGISTER_TIMEOUT  = 4
	_SYN_DIAL_TIMEOUT      = 2
	_SYN_DEFAULT_NODE_PORT = 8001
)

type ringmgr struct {
	r ring.Ring
	b *ring.Builder
	sync.RWMutex
	slaves       []*RingSlave
	version      int64
	localAddress string
	rb           *[]byte // even a 1000 node ring is reasonably small (17k) so just keep the current ring in mem
	bb           *[]byte
	netlimits    []*net.IPNet
	tierlimits   []string
}

type RingSlave struct {
	sync.RWMutex
	status  bool
	last    time.Time
	version int64
	addr    string
	conn    *grpc.ClientConn
	client  pb.RingDistClient
}

func (s *ringmgr) loadRingBuilderBytes(version int64) (ring, builder *[]byte, err error) {
	b, err := ioutil.ReadFile(fmt.Sprintf("/etc/ort/%d-ort.builder", version))
	if err != nil {
		return ring, builder, err
	}
	r, err := ioutil.ReadFile(fmt.Sprintf("/etc/ort/%d-ort.ring", version))
	if err != nil {
		return ring, builder, err
	}
	return &r, &b, nil
}

func (s *ringmgr) RegisterSlave(slave *RingSlave) error {
	log.Printf("--> Attempting to register: %+v", slave)
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithBlock())
	opts = append(opts, grpc.WithTimeout(_SYN_DIAL_TIMEOUT*time.Second))
	var err error
	slave.conn, err = grpc.Dial(slave.addr, opts...)
	if err != nil {
		return err
	}
	slave.client = pb.NewRingDistClient(slave.conn)
	log.Printf("--> Setting up slave: %s", slave.addr)
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(_SYN_REGISTER_TIMEOUT)*time.Second)
	i := &pb.RingMsg{
		Version:  s.version,
		Ring:     *s.rb,
		Builder:  *s.bb,
		Deadline: 0,
		Rollback: 0,
	}
	res, err := slave.client.Setup(ctx, i)
	if err != nil {
		return err
	}
	if res.Version != s.version {
		return fmt.Errorf("Version or master on remote node %+v did not match local entries. Got %+v.", slave, res)
	}
	if !res.Ring || !res.Builder {
		log.Printf("res is: %#v\n", res)
		return fmt.Errorf("Slave failed to store ring or builder: %s", res.ErrMsg)
	}
	log.Printf("<-- Slave response: %+v", res)
	slave.version = res.Version
	slave.last = time.Now()
	slave.status = true
	log.Printf("--> Slave state is now: %+v\n", slave)
	return nil
}

//TODO: Need concurrency, we should just fire of replicates in goroutines
// and collects the results. On a failure we still need to send the rollback
// or have the slave's commit deadline trigger.
func (s *ringmgr) replicateRing(r ring.Ring, rb, bb *[]byte) error {
	failcount := 0
	for _, slave := range s.slaves {
		ctx, _ := context.WithTimeout(context.Background(), time.Duration(_SYN_REGISTER_TIMEOUT)*time.Second)
		i := &pb.RingMsg{
			Version:  r.Version(),
			Ring:     *rb,
			Builder:  *bb,
			Deadline: time.Now().Add(60 * time.Second).Unix(),
			Rollback: s.version,
		}
		res, err := slave.client.Store(ctx, i)
		if err != nil {
			log.Println(err)
			failcount++
			continue
		}
		if res.Version != r.Version() {
			log.Printf("Version or master on remote node %+v did not match local entries. Got %+v.", slave, res)
			failcount++
			continue
		}
		if !res.Ring || !res.Builder {
			log.Printf("res is: %#v\n", res)
			log.Printf("Slave failed to store ring or builder: %s", res.ErrMsg)
			failcount++
			continue
		}
		log.Printf("<-- Slave response: %+v", res)
		slave.version = res.Version
		slave.last = time.Now()
		slave.status = true
	}
	if failcount > (len(s.slaves) / 2) {
		return fmt.Errorf("Failed to get replication majority")
	}
	return nil
}

// TODO: Need field/value error checks
func (s *ringmgr) AddNode(c context.Context, e *pb.Node) (*pb.RingStatus, error) {
	s.Lock()
	defer s.Unlock()
	log.Println("--> Got AddNode request")
	n := s.b.AddNode(e.Active, e.Capacity, e.Tiers, e.Addresses, e.Meta, e.Conf)
	report := [][]string{
		[]string{"ID:", fmt.Sprintf("%016x", n.ID())},
		[]string{"RAW ID", fmt.Sprintf("%d", n.ID())},
		[]string{"Active:", fmt.Sprintf("%v", n.Active())},
		[]string{"Capacity:", fmt.Sprintf("%d", n.Capacity())},
		[]string{"Tiers:", strings.Join(n.Tiers(), "\n")},
		[]string{"Addresses:", strings.Join(n.Addresses(), "\n")},
		[]string{"Meta:", n.Meta()},
		[]string{"Conf:", fmt.Sprintf("%s", n.Conf())},
	}
	newRing := s.b.Ring()
	newVersion := newRing.Version()
	log.Println("--> New ring version is:", newRing.Version())
	if err := ring.PersistRingOrBuilder(nil, s.b, fmt.Sprintf("/etc/ort/%d-ort.builder", newVersion)); err != nil {
		return &pb.RingStatus{}, err
	}
	if err := ring.PersistRingOrBuilder(newRing, nil, fmt.Sprintf("/etc/ort/%d-ort.ring", newVersion)); err != nil {
		return &pb.RingStatus{}, err
	}

	newRB, newBB, err := s.loadRingBuilderBytes(newVersion)
	if err != nil {
		return &pb.RingStatus{}, fmt.Errorf("Failed to load new ring/builder bytes:", err)
	}

	err = s.replicateRing(newRing, newRB, newBB)
	if err != nil {
		return &pb.RingStatus{}, fmt.Errorf("Ring replicate failed:", err)
	}

	s.r = newRing
	s.version = s.r.Version()
	log.Print(brimtext.Align(report, nil))
	return &pb.RingStatus{Status: true, Version: s.version}, nil
}

func (s *ringmgr) RemoveNode(c context.Context, n *pb.Node) (*pb.RingStatus, error) {
	return &pb.RingStatus{}, nil
}

func (s *ringmgr) ModNode(c context.Context, n *pb.ModifyMsg) (*pb.RingStatus, error) {
	return &pb.RingStatus{}, nil
}

func (s *ringmgr) SetConf(c context.Context, n *pb.Conf) (*pb.RingStatus, error) {
	return &pb.RingStatus{true, s.version}, nil
}

func (s *ringmgr) SetActive(c context.Context, n *pb.Node) (*pb.RingStatus, error) {
	return &pb.RingStatus{true, s.version}, nil
}

func (s *ringmgr) GetVersion(c context.Context, n *pb.EmptyMsg) (*pb.RingStatus, error) {
	s.RLock()
	defer s.RUnlock()
	return &pb.RingStatus{true, s.version}, nil
}

// validNodeIP verifies that the provided ip is not a loopback or multicast address
// and checks whether the ip is in the configured network limits range.
func (s *ringmgr) validNodeIP(i net.IP) bool {
	switch {
	case i.IsLoopback():
		return false
	case i.IsMulticast():
		return false
	}
	inRange := false
	for _, n := range s.netlimits {
		if n.Contains(i) {
			inRange = true
		}
	}
	return inRange
}

// validTiers parses a list of provided ring tiers to see if they are valid.
// Its assumed that there must be at least 2 tiers
// tier0 must never already exist as a tier0 entry in the ring
// tier1+ just need to match the configured go regex filter
func (s *ringmgr) validTiers(t []string) bool {
	if len(t) <= 1 {
		return false
	}
	r, err := s.r.Nodes().Filter([]string{fmt.Sprintf("tier0=%s", t[0])})
	if len(r) != 0 || err != nil {
		return false
	}
	for i := 1; i <= len(t); i++ {
		for _, v := range s.tierlimits {
			matched, err := regexp.MatchString(v, t[i])
			if err != nil {
				return false
			}
			if matched {
				return true
			}
		}
	}
	return false
}

// nodeInRing just checks to see if the hostname or addresses appear
// in any existing entries meta or address fields.
func (s *ringmgr) nodeInRing(hostname string, addrs []string) bool {
	a := strings.Join(addrs, "|")
	r, _ := s.r.Nodes().Filter([]string{fmt.Sprintf("meta~=%s.*", hostname), fmt.Sprintf("address~=%s", a)})
	if len(r) != 0 {
		return true
	}
	return false
}

func (s *ringmgr) RegisterNode(c context.Context, r *pb.RegisterRequest) (*pb.NodeConfig, error) {
	s.Lock()
	defer s.Unlock()
	log.Println("got request")
	log.Printf("%#v", r)

	var addrs []string

	for _, v := range r.Addrs {
		i, _, err := net.ParseCIDR(v)
		if err != nil {
			log.Println("Encountered unknown network addr", v, err)
			continue
		}
		if s.validNodeIP(i) {
			addrs = append(addrs, fmt.Sprintf("%s:%d", i.String(), _SYN_DEFAULT_NODE_PORT))
		}
	}
	switch {
	case len(addrs) == 0:
		log.Println("Host provided no valid addresses during registration.")
		return &pb.NodeConfig{}, fmt.Errorf("No valid addresses provided")
	case s.nodeInRing(r.Hostname, addrs):
		log.Println("Node already appears to be in ring")
		return &pb.NodeConfig{}, fmt.Errorf("Node already in ring")
	case !s.validTiers(r.Tiers):
		return &pb.NodeConfig{}, fmt.Errorf("Invalid tiers provided")
	}

	n := s.b.AddNode(true, 1000, r.Tiers, addrs, fmt.Sprintf("%s|%s", r.Hostname, r.Hardwareid), []byte(""))
	report := [][]string{
		[]string{"ID:", fmt.Sprintf("%016x", n.ID())},
		[]string{"RAW ID", fmt.Sprintf("%d", n.ID())},
		[]string{"Active:", fmt.Sprintf("%v", n.Active())},
		[]string{"Capacity:", fmt.Sprintf("%d", n.Capacity())},
		[]string{"Tiers:", strings.Join(n.Tiers(), "\n")},
		[]string{"Addresses:", strings.Join(n.Addresses(), "\n")},
		[]string{"Meta:", n.Meta()},
		[]string{"Conf:", fmt.Sprintf("%s", n.Conf())},
	}
	newRing := s.b.Ring()
	newVersion := newRing.Version()
	log.Println("--> New ring version is:", newRing.Version())
	if err := ring.PersistRingOrBuilder(nil, s.b, fmt.Sprintf("/etc/ort/%d-ort.builder", newVersion)); err != nil {
		return &pb.NodeConfig{}, err
	}
	if err := ring.PersistRingOrBuilder(newRing, nil, fmt.Sprintf("/etc/ort/%d-ort.ring", newVersion)); err != nil {
		return &pb.NodeConfig{}, err
	}

	newRB, newBB, err := s.loadRingBuilderBytes(newVersion)
	if err != nil {
		return &pb.NodeConfig{}, fmt.Errorf("Failed to load new ring/builder bytes:", err)
	}

	err = s.replicateRing(newRing, newRB, newBB)
	if err != nil {
		return &pb.NodeConfig{}, fmt.Errorf("Ring replicate failed:", err)
	}

	s.r = newRing
	s.bb = newBB
	s.rb = newRB
	s.version = s.r.Version()
	log.Print(brimtext.Align(report, nil))

	return &pb.NodeConfig{Localid: n.ID(), Ring: *s.rb}, nil
}
