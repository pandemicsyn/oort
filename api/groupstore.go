package api

import (
	"crypto/tls"
	"errors"
	"fmt"
	"sync"

	"github.com/gholt/store"
	"github.com/pandemicsyn/oort/api/groupproto"
	"github.com/pandemicsyn/oort/api/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// TODO: I'm unsure on the handling of grpc errors; the ones grpc has, not that
// it passes along. Should we disconnect everything on any grpc error, assuming
// the transit is corrupted and therefore a reconnect required? Or can grpc
// recover on its own? Also, should we retry requests that are idempotent?
// Probably better to let the caller decide on the retry logic.

// TODO: Background() calls should be replaced with a "real" contexts with
// deadlines, etc.

// TODO: I lock while asking the grpc client to make any stream. I'm not sure
// if this is required. Needs testing.

// TODO: We should consider using templatized code for this and valuestore.go

type groupStore struct {
	lock               sync.Mutex
	addr               string
	insecureSkipVerify bool
	opts               []grpc.DialOption
	creds              credentials.TransportAuthenticator
	conn               *grpc.ClientConn
	client             groupproto.GroupStoreClient
	lookupStreams      chan groupproto.GroupStore_StreamLookupClient
	lookupGroupStreams chan groupproto.GroupStore_StreamLookupGroupClient
	readStreams        chan groupproto.GroupStore_StreamReadClient
	writeStreams       chan groupproto.GroupStore_StreamWriteClient
	deleteStreams      chan groupproto.GroupStore_StreamDeleteClient
	readGroupStreams   chan groupproto.GroupStore_StreamReadGroupClient
}

// NewGroupStore creates a GroupStore connection via grpc to the given address;
// note that Startup() will have been called in the returned store, so calling
// Startup() yourself is optional.
func NewGroupStore(addr string, streams int, insecureSkipVerify bool, opts ...grpc.DialOption) (store.GroupStore, error) {
	g := &groupStore{
		addr:               addr,
		insecureSkipVerify: insecureSkipVerify,
		opts:               opts,
		creds:              credentials.NewTLS(&tls.Config{InsecureSkipVerify: insecureSkipVerify}),
	}
	g.opts = append(g.opts, grpc.WithTransportCredentials(g.creds))
	g.lookupStreams = make(chan groupproto.GroupStore_StreamLookupClient, streams)
	g.lookupGroupStreams = make(chan groupproto.GroupStore_StreamLookupGroupClient, streams)
	g.readStreams = make(chan groupproto.GroupStore_StreamReadClient, streams)
	g.writeStreams = make(chan groupproto.GroupStore_StreamWriteClient, streams)
	g.deleteStreams = make(chan groupproto.GroupStore_StreamDeleteClient, streams)
	g.readGroupStreams = make(chan groupproto.GroupStore_StreamReadGroupClient, streams)
	return g, g.Startup()
}

func (g *groupStore) Startup() error {
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
	for i := cap(g.lookupStreams); i > 0; i-- {
		g.lookupStreams <- nil
	}
	for i := cap(g.lookupGroupStreams); i > 0; i-- {
		g.lookupGroupStreams <- nil
	}
	for i := cap(g.readStreams); i > 0; i-- {
		g.readStreams <- nil
	}
	for i := cap(g.writeStreams); i > 0; i-- {
		g.writeStreams <- nil
	}
	for i := cap(g.deleteStreams); i > 0; i-- {
		g.deleteStreams <- nil
	}
	for i := cap(g.readGroupStreams); i > 0; i-- {
		g.readGroupStreams <- nil
	}
	return nil
}

func (g *groupStore) Shutdown() error {
	g.lock.Lock()
	defer g.lock.Unlock()
	if g.conn == nil {
		return nil
	}
	g.conn.Close()
	g.conn = nil
	g.client = nil
	for i := cap(g.lookupStreams); i > 0; i-- {
		<-g.lookupStreams
	}
	for i := cap(g.lookupGroupStreams); i > 0; i-- {
		<-g.lookupGroupStreams
	}
	for i := cap(g.readStreams); i > 0; i-- {
		<-g.readStreams
	}
	for i := cap(g.writeStreams); i > 0; i-- {
		<-g.writeStreams
	}
	for i := cap(g.deleteStreams); i > 0; i-- {
		<-g.deleteStreams
	}
	for i := cap(g.readGroupStreams); i > 0; i-- {
		<-g.readGroupStreams
	}
	return nil
}

func (g *groupStore) EnableWrites() error {
	return nil
}

func (g *groupStore) DisableWrites() error {
	// TODO: I suppose we could implement toggling writes from this client;
	// I'll leave that for later.
	return errors.New("cannot disable writes with this client at this time")
}

func (g *groupStore) Flush() error {
	// Nothing cached on this end, so nothing to flush.
	return nil
}

func (g *groupStore) AuditPass() error {
	return errors.New("audit passes not available with this client at this time")
}

type s struct{}

func (*s) String() string {
	return "stats not available with this client at this time"
}

var noStats = &s{}

func (g *groupStore) Stats(debug bool) (fmt.Stringer, error) {
	return noStats, nil
}

func (g *groupStore) ValueCap() (uint32, error) {
	// TODO: This should be a (cached) value from the server. Servers don't
	// change their value caps on the fly, so the cache can be kept until
	// disconnect.
	return 0xffffffff, nil
}

func (g *groupStore) Lookup(parentKeyA, parentKeyB, childKeyA, childKeyB uint64) (timestampmicro int64, length uint32, err error) {
	s := <-g.lookupStreams
	if s == nil {
		g.lock.Lock()
		s, err = g.client.StreamLookup(context.Background())
		g.lock.Unlock()
		if err != nil {
			g.lookupStreams <- nil
			return 0, 0, err
		}
	}
	req := &groupproto.LookupRequest{
		KeyA:      parentKeyA,
		KeyB:      parentKeyB,
		ChildKeyA: childKeyA,
		ChildKeyB: childKeyB,
	}
	if err = s.Send(req); err != nil {
		g.lookupStreams <- nil
		return 0, 0, err
	}
	res, err := s.Recv()
	if err != nil {
		g.lookupStreams <- nil
		return 0, 0, err
	}
	if res.Err != "" {
		err = proto.TranslateErrorString(res.Err)
	}
	g.lookupStreams <- s
	return res.TimestampMicro, res.Length, err
}

func (g *groupStore) LookupGroup(parentKeyA, parentKeyB uint64) ([]store.LookupGroupItem, error) {
	var err error
	s := <-g.lookupGroupStreams
	if s == nil {
		g.lock.Lock()
		s, err = g.client.StreamLookupGroup(context.Background())
		g.lock.Unlock()
		if err != nil {
			g.lookupGroupStreams <- nil
			return nil, err
		}
	}
	req := &groupproto.LookupGroupRequest{
		KeyA: parentKeyA,
		KeyB: parentKeyB,
	}
	if err = s.Send(req); err != nil {
		g.lookupGroupStreams <- nil
		return nil, err
	}
	res, err := s.Recv()
	if err != nil {
		g.lookupGroupStreams <- nil
		return nil, err
	}
	rv := make([]store.LookupGroupItem, len(res.Items))
	for i, v := range res.Items {
		rv[i].ChildKeyA = v.ChildKeyA
		rv[i].ChildKeyB = v.ChildKeyB
		rv[i].TimestampMicro = v.TimestampMicro
		rv[i].Length = v.Length
	}
	if res.Err != "" {
		err = proto.TranslateErrorString(res.Err)
	}
	g.lookupGroupStreams <- s
	return rv, err
}

func (g *groupStore) Read(parentKeyA, parentKeyB, childKeyA, childKeyB uint64, value []byte) (timestampmicro int64, rvalue []byte, err error) {
	rvalue = value
	s := <-g.readStreams
	if s == nil {
		g.lock.Lock()
		s, err = g.client.StreamRead(context.Background())
		g.lock.Unlock()
		if err != nil {
			g.readStreams <- nil
			return 0, rvalue, err
		}
	}
	req := &groupproto.ReadRequest{
		KeyA:      parentKeyA,
		KeyB:      parentKeyB,
		ChildKeyA: childKeyA,
		ChildKeyB: childKeyB,
	}
	if err = s.Send(req); err != nil {
		g.readStreams <- nil
		return 0, rvalue, err
	}
	res, err := s.Recv()
	if err != nil {
		g.readStreams <- nil
		return 0, rvalue, err
	}
	rvalue = append(rvalue, res.Value...)
	if res.Err != "" {
		err = proto.TranslateErrorString(res.Err)
	}
	g.readStreams <- s
	return res.TimestampMicro, rvalue, err
}

func (g *groupStore) Write(parentKeyA, parentKeyB, childKeyA, childKeyB uint64, timestampmicro int64, value []byte) (oldtimestampmicro int64, err error) {
	s := <-g.writeStreams
	if s == nil {
		g.lock.Lock()
		s, err = g.client.StreamWrite(context.Background())
		g.lock.Unlock()
		if err != nil {
			g.writeStreams <- nil
			return 0, err
		}
	}
	req := &groupproto.WriteRequest{
		KeyA:           parentKeyA,
		KeyB:           parentKeyB,
		ChildKeyA:      childKeyA,
		ChildKeyB:      childKeyB,
		TimestampMicro: timestampmicro,
		Value:          value,
	}
	if err = s.Send(req); err != nil {
		g.writeStreams <- nil
		return 0, err
	}
	res, err := s.Recv()
	if err != nil {
		g.writeStreams <- nil
		return 0, err
	}
	if res.Err != "" {
		err = proto.TranslateErrorString(res.Err)
	}
	g.writeStreams <- s
	return res.TimestampMicro, err
}

func (g *groupStore) Delete(parentKeyA, parentKeyB, childKeyA, childKeyB uint64, timestampmicro int64) (oldtimestampmicro int64, err error) {
	s := <-g.deleteStreams
	if s == nil {
		g.lock.Lock()
		s, err = g.client.StreamDelete(context.Background())
		g.lock.Unlock()
		if err != nil {
			g.deleteStreams <- nil
			return 0, err
		}
	}
	req := &groupproto.DeleteRequest{
		KeyA:           parentKeyA,
		KeyB:           parentKeyB,
		ChildKeyA:      childKeyA,
		ChildKeyB:      childKeyB,
		TimestampMicro: timestampmicro,
	}
	if err = s.Send(req); err != nil {
		g.deleteStreams <- nil
		return 0, err
	}
	res, err := s.Recv()
	if err != nil {
		g.deleteStreams <- nil
		return 0, err
	}
	if res.Err != "" {
		err = proto.TranslateErrorString(res.Err)
	}
	g.deleteStreams <- s
	return res.TimestampMicro, err
}

func (g *groupStore) ReadGroup(parentKeyA, parentKeyB uint64) ([]store.ReadGroupItem, error) {
	var err error
	s := <-g.readGroupStreams
	if s == nil {
		g.lock.Lock()
		s, err = g.client.StreamReadGroup(context.Background())
		g.lock.Unlock()
		if err != nil {
			g.readGroupStreams <- nil
			return nil, err
		}
	}
	req := &groupproto.ReadGroupRequest{
		KeyA: parentKeyA,
		KeyB: parentKeyB,
	}
	if err = s.Send(req); err != nil {
		g.readGroupStreams <- nil
		return nil, err
	}
	res, err := s.Recv()
	if err != nil {
		g.readGroupStreams <- nil
		return nil, err
	}
	rv := make([]store.ReadGroupItem, len(res.Items))
	for i, v := range res.Items {
		rv[i].ChildKeyA = v.ChildKeyA
		rv[i].ChildKeyB = v.ChildKeyB
		rv[i].TimestampMicro = v.TimestampMicro
		rv[i].Value = v.Value
	}
	g.readGroupStreams <- s
	return rv, nil
}
