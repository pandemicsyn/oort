package api

import (
	"crypto/tls"
	"errors"
	"fmt"
	"sync"

	"github.com/gholt/store"
	"github.com/pandemicsyn/oort/api/proto"
	"github.com/pandemicsyn/oort/api/valueproto"
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

type valueStore struct {
	lock               sync.Mutex
	addr               string
	insecureSkipVerify bool
	opts               []grpc.DialOption
	creds              credentials.TransportAuthenticator
	conn               *grpc.ClientConn
	client             valueproto.ValueStoreClient
	lookupStreams      chan valueproto.ValueStore_StreamLookupClient
	readStreams        chan valueproto.ValueStore_StreamReadClient
	writeStreams       chan valueproto.ValueStore_StreamWriteClient
	deleteStreams      chan valueproto.ValueStore_StreamDeleteClient
}

func NewValueStore(addr string, streams int, insecureSkipVerify bool, opts ...grpc.DialOption) (store.ValueStore, error) {
	g := &valueStore{
		addr:               addr,
		insecureSkipVerify: insecureSkipVerify,
		opts:               opts,
		creds:              credentials.NewTLS(&tls.Config{InsecureSkipVerify: insecureSkipVerify}),
	}
	g.opts = append(g.opts, grpc.WithTransportCredentials(g.creds))
	g.lookupStreams = make(chan valueproto.ValueStore_StreamLookupClient, streams)
	g.readStreams = make(chan valueproto.ValueStore_StreamReadClient, streams)
	g.writeStreams = make(chan valueproto.ValueStore_StreamWriteClient, streams)
	g.deleteStreams = make(chan valueproto.ValueStore_StreamDeleteClient, streams)
	return g, nil
}

func (g *valueStore) Startup() error {
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
	g.client = valueproto.NewValueStoreClient(g.conn)
	for i := cap(g.lookupStreams); i > 0; i-- {
		g.lookupStreams <- nil
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
	return nil
}

func (g *valueStore) Shutdown() error {
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
	for i := cap(g.readStreams); i > 0; i-- {
		<-g.readStreams
	}
	for i := cap(g.writeStreams); i > 0; i-- {
		<-g.writeStreams
	}
	for i := cap(g.deleteStreams); i > 0; i-- {
		<-g.deleteStreams
	}
	return nil
}

func (g *valueStore) EnableWrites() error {
	return nil
}

func (g *valueStore) DisableWrites() error {
	// TODO: I suppose we could implement toggling writes from this client;
	// I'll leave that for later.
	return errors.New("cannot disable writes with this client at this time")
}

func (g *valueStore) Flush() error {
	// Nothing cached on this end, so nothing to flush.
	return nil
}

func (g *valueStore) AuditPass() error {
	return errors.New("audit passes not available with this client at this time")
}

func (g *valueStore) Stats(debug bool) (fmt.Stringer, error) {
	return noStats, nil
}

func (g *valueStore) ValueCap() (uint32, error) {
	// TODO: This should be a (cached) value from the server. Servers don't
	// change their value caps on the fly, so the cache can be kept until
	// disconnect.
	return 0xffffffff, nil
}

func (g *valueStore) Lookup(keyA, keyB uint64) (timestampmicro int64, length uint32, err error) {
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
	req := &valueproto.LookupRequest{
		KeyA: keyA,
		KeyB: keyB,
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

func (g *valueStore) Read(keyA, keyB uint64, value []byte) (timestampmicro int64, rvalue []byte, err error) {
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
	req := &valueproto.ReadRequest{
		KeyA: keyA,
		KeyB: keyB,
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

func (g *valueStore) Write(keyA, keyB uint64, timestampmicro int64, value []byte) (oldtimestampmicro int64, err error) {
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
	req := &valueproto.WriteRequest{
		KeyA:           keyA,
		KeyB:           keyB,
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

func (g *valueStore) Delete(keyA, keyB uint64, timestampmicro int64) (oldtimestampmicro int64, err error) {
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
	req := &valueproto.DeleteRequest{
		KeyA:           keyA,
		KeyB:           keyB,
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
