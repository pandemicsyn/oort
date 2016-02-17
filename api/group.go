package api

import (
	"crypto/tls"
	"errors"
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
	ReadGroup(parentKeyA, parentKeyB uint64) ([]*ReadGroupItem, error)
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
	streamRead         groupproto.GroupStore_StreamReadClient
	streamWrite        groupproto.GroupStore_StreamWriteClient
	streamDelete       groupproto.GroupStore_StreamDeleteClient
	streamReadGroup    groupproto.GroupStore_StreamReadGroupClient
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

func (g *group) Shutdown() error {
	g.lock.Lock()
	defer g.lock.Unlock()
	if g.conn == nil {
		return nil
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
	g.streamRead = nil
	g.streamWrite = nil
	g.streamDelete = nil
	g.streamReadGroup = nil
	return nil
}

func (g *group) EnableWrites() error {
	return nil
}

func (g *group) DisableWrites() error {
	// TODO: I suppose we could implement toggling writes from this client;
	// I'll leave that for later.
	return errors.New("cannot disable writes with this client at this time")
}

func (g *group) Flush() error {
	// Nothing cached on this end, so nothing to flush.
	return nil
}

func (g *group) AuditPass() error {
	return errors.New("audit passes not available with this client at this time")
}

type s struct{}

func (*s) String() string { return "" }

func (g *group) Stats(debug bool) (fmt.Stringer, error) {
	return nil, errors.New("stats not available with this client at this time")
}

func (g *group) ValueCap() (uint32, error) {
	// TODO: This should be a (cached) value from the server. Servers don't
	// change their value caps on the fly, so the cache can be kept until
	// disconnect.
	return 0xffffffff, nil
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
	if res.Err != "" {
		err = errors.New(res.Err)
	}
	return res.TimestampMicro, res.Length, err
}

func (g *group) LookupGroup(parentKeyA, parentKeyB uint64) ([]*store.LookupGroupItem, error) {
	var err error
	g.lock.RLock()
	if g.streamLookupGroup == nil {
		// TODO: Background should probably be replaced with a "real" context
		// with deadlines, etc.
		g.streamLookupGroup, err = g.client.StreamLookupGroup(context.Background())
		if err != nil {
			g.streamLookupGroup = nil
			g.lock.RUnlock()
			return nil, err
		}
	}
	s := g.streamLookupGroup
	g.lock.RUnlock()
	req := &groupproto.LookupGroupRequest{
		KeyA: parentKeyA,
		KeyB: parentKeyB,
	}
	if err = s.Send(req); err != nil {
		return nil, err
	}
	res, err := s.Recv()
	if err != nil {
		return nil, err
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
	if res.Err != "" {
		err = errors.New(res.Err)
	}
	return rv, err
}

func (g *group) Read(parentKeyA, parentKeyB, childKeyA, childKeyB uint64, value []byte) (timestampmicro int64, rvalue []byte, err error) {
	rvalue = value
	g.lock.RLock()
	if g.streamRead == nil {
		// TODO: Background should probably be replaced with a "real" context
		// with deadlines, etc.
		g.streamRead, err = g.client.StreamRead(context.Background())
		if err != nil {
			g.streamRead = nil
			g.lock.RUnlock()
			return 0, rvalue, err
		}
	}
	s := g.streamRead
	g.lock.RUnlock()
	req := &groupproto.ReadRequest{
		KeyA:      parentKeyA,
		KeyB:      parentKeyB,
		ChildKeyA: childKeyA,
		ChildKeyB: childKeyB,
	}
	if err = s.Send(req); err != nil {
		return 0, rvalue, err
	}
	res, err := s.Recv()
	if err != nil {
		return 0, rvalue, err
	}
	rvalue = append(rvalue, res.Value...)
	if res.Err != "" {
		// TODO: I want to translate the errors into the "proper" error types,
		// as defined by the store package.
		err = errors.New(res.Err)
	}
	return res.TimestampMicro, rvalue, err
}

func (g *group) Write(parentKeyA, parentKeyB, childKeyA, childKeyB uint64, timestampmicro int64, value []byte) (oldtimestampmicro int64, err error) {
	g.lock.RLock()
	if g.streamWrite == nil {
		// TODO: Background should probably be replaced with a "real" context
		// with deadlines, etc.
		g.streamWrite, err = g.client.StreamWrite(context.Background())
		if err != nil {
			g.streamWrite = nil
			g.lock.RUnlock()
			return 0, err
		}
	}
	s := g.streamWrite
	g.lock.RUnlock()
	req := &groupproto.WriteRequest{
		KeyA:           parentKeyA,
		KeyB:           parentKeyB,
		ChildKeyA:      childKeyA,
		ChildKeyB:      childKeyB,
		TimestampMicro: timestampmicro,
		Value:          value,
	}
	if err = s.Send(req); err != nil {
		return 0, err
	}
	res, err := s.Recv()
	if err != nil {
		return 0, err
	}
	if res.Err != "" {
		err = errors.New(res.Err)
	}
	return res.TimestampMicro, err
}

func (g *group) Delete(parentKeyA, parentKeyB, childKeyA, childKeyB uint64, timestampmicro int64) (oldtimestampmicro int64, err error) {
	g.lock.RLock()
	if g.streamDelete == nil {
		// TODO: Background should probably be replaced with a "real" context
		// with deadlines, etc.
		g.streamDelete, err = g.client.StreamDelete(context.Background())
		if err != nil {
			g.streamDelete = nil
			g.lock.RUnlock()
			return 0, err
		}
	}
	s := g.streamDelete
	g.lock.RUnlock()
	req := &groupproto.DeleteRequest{
		KeyA:           parentKeyA,
		KeyB:           parentKeyB,
		ChildKeyA:      childKeyA,
		ChildKeyB:      childKeyB,
		TimestampMicro: timestampmicro,
	}
	if err = s.Send(req); err != nil {
		return 0, err
	}
	res, err := s.Recv()
	if err != nil {
		return 0, err
	}
	if res.Err != "" {
		err = errors.New(res.Err)
	}
	return res.TimestampMicro, err
}

func (g *group) ReadGroup(parentKeyA, parentKeyB uint64) ([]*ReadGroupItem, error) {
	var err error
	g.lock.RLock()
	if g.streamReadGroup == nil {
		// TODO: Background should probably be replaced with a "real" context
		// with deadlines, etc.
		g.streamReadGroup, err = g.client.StreamReadGroup(context.Background())
		if err != nil {
			g.streamReadGroup = nil
			g.lock.RUnlock()
			return nil, err
		}
	}
	s := g.streamReadGroup
	g.lock.RUnlock()
	req := &groupproto.ReadGroupRequest{
		KeyA: parentKeyA,
		KeyB: parentKeyB,
	}
	if err = s.Send(req); err != nil {
		return nil, err
	}
	res, err := s.Recv()
	if err != nil {
		return nil, err
	}
	rv := make([]*ReadGroupItem, len(res.Items))
	for i, v := range res.Items {
		rv[i] = &ReadGroupItem{
			ChildKeyA:      v.ChildKeyA,
			ChildKeyB:      v.ChildKeyB,
			TimestampMicro: v.TimestampMicro,
			Value:          v.Value,
		}
	}
	return rv, nil
}
