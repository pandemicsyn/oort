package api

import (
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/gholt/store"
	"github.com/pandemicsyn/oort/api/groupproto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Group interface {
	store.GroupStore
	ReadGroup(parentKeyA, parentKeyB uint64) []ReadGroupItem
}

type ReadGroupItem struct {
	ChildKeyA      uint64
	ChildKeyB      uint64
	TimestampMicro int64
	Value          []byte
}

type group struct {
	lock               sync.RWMutex
	addr               string
	insecureSkipVerify bool
	opts               []grpc.DialOption
	creds              credentials.TransportAuthenticator
	conn               *grpc.ClientConn
	client             groupproto.GroupStoreClient
	streamLookup       groupproto.GroupStore_StreamLookupClient
	streamLookupGroup  groupproto.GroupStore_StreamLookupGroupClient
}

func NewGroup(addr string, insecureSkipVerify bool, opts ...grpc.DialOption) (Group, error) {
	g := &group{
		addr:               addr,
		insecureSkipVerify: insecureSkipVerify,
		opts:               opts,
		creds:              credentials.NewTLS(&tls.Config{InsecureSkipVerify: insecureSkipVerify}),
	}
	g.opts = append(g.opts, grpc.WithTransportCredentials(g.creds))
	return g, nil
}

func (g *group) Startup() error {
	g.lock.Lock()
	defer g.lock.Unlock()
	if g.conn != nil {
		return nil
	}
	var err error
	g.conn, err = grpc.Dial(g.addr, g.opts...)
	if err != nil {
		g.conn = nil
		return err
	}
	g.client = groupproto.NewGroupStoreClient(g.conn)
	return nil
}

func (g *group) Shutdown() {
	g.lock.Lock()
	defer g.lock.Unlock()
	if g.conn == nil {
		return
	}
	// I don't know that we need to close the streams really; but if we do it'd
	// look something like this:
	// if g.streamLookup != nil {
	//     g.streamLookup.CloseSend()
	//     g.streamLookup = nil
	// }
	g.conn.Close()
	g.conn = nil
	g.client = nil
	g.streamLookup = nil
	g.streamLookupGroup = nil
}

func (g *group) EnableWrites() {
}

func (g *group) DisableWrites() {
	// TODO: I suppose we could implement toggling writes from this client;
	// I'll leave that for later.
}

func (g *group) Flush() {
}

func (g *group) AuditPass() {
}

type s struct{}

func (*s) String() string { return "" }

func (g *group) Stats(debug bool) fmt.Stringer {
	return &s{}
}

func (g *group) ValueCap() uint32 {
	// TODO: This should be a (cached) value from the server.
	return 0xffffffff
}

func (g *group) Lookup(parentKeyA, parentKeyB, childKeyA, childKeyB uint64) (timestampmicro int64, length uint32, err error) {
	g.lock.RLock()
	if g.streamLookup == nil {
		// TODO: Background should probably be replaced with a "real" context
		// with deadlines, etc.
		g.streamLookup, err = g.client.StreamLookup(context.Background())
		if err != nil {
			g.streamLookup = nil
			g.lock.RUnlock()
			return 0, 0, err
		}
	}
	s := g.streamLookup
	g.lock.RUnlock()
	req := &groupproto.LookupRequest{
		KeyA:      parentKeyA,
		KeyB:      parentKeyB,
		ChildKeyA: childKeyA,
		ChildKeyB: childKeyB,
	}
	if err = s.Send(req); err != nil {
		return 0, 0, err
	}
	res, err := s.Recv()
	if err != nil {
		return 0, 0, err
	}
	return res.TimestampMicro, res.Length, nil
}

func (g *group) LookupGroup(parentKeyA, parentKeyB uint64) []*store.LookupGroupItem {
	var err error
	g.lock.RLock()
	if g.streamLookupGroup == nil {
		// TODO: Background should probably be replaced with a "real" context
		// with deadlines, etc.
		g.streamLookupGroup, err = g.client.StreamLookupGroup(context.Background())
		if err != nil {
			g.streamLookupGroup = nil
			g.lock.RUnlock()
			return nil
		}
	}
	s := g.streamLookupGroup
	g.lock.RUnlock()
	req := &groupproto.LookupGroupRequest{
		KeyA: parentKeyA,
		KeyB: parentKeyB,
	}
	if err = s.Send(req); err != nil {
		return nil
	}
	res, err := s.Recv()
	if err != nil {
		return nil
	}
	rv := make([]*store.LookupGroupItem, len(res.Items))
	for i, v := range res.Items {
		rv[i] = &store.LookupGroupItem{
			ChildKeyA:      v.ChildKeyA,
			ChildKeyB:      v.ChildKeyB,
			TimestampMicro: v.TimestampMicro,
			Length:         v.Length,
		}
	}
	return rv
}

func (g *group) Read(parentKeyA, parentKeyB, childKeyA, childKeyB uint64, value []byte) (timestampmicro int64, rvalue []byte, err error) {
	return 0, nil, nil
}

func (g *group) Write(parentKeyA, parentKeyB, childKeyA, childKeyB uint64, timestampmicro int64, value []byte) (oldtimestampmicro int64, err error) {
	return 0, nil
}

func (g *group) Delete(parentKeyA, parentKeyB, childKeyA, childKeyB uint64, timestampmicro int64) (oldtimestampmicro int64, err error) {
	return 0, nil
}

func (g *group) ReadGroup(parentKeyA, parentKeyB uint64) []ReadGroupItem {
	return nil
}
