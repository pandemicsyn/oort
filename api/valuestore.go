package api

import (
	"errors"
	"fmt"
	"sync"

	"github.com/gholt/store"
	"github.com/pandemicsyn/oort/api/proto"
	"github.com/pandemicsyn/oort/api/valueproto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
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
	lock          sync.Mutex
	addr          string
	opts          []grpc.DialOption
	conn          *grpc.ClientConn
	client        valueproto.ValueStoreClient
	lookupStreams chan valueproto.ValueStore_StreamLookupClient
	readStreams   chan valueproto.ValueStore_StreamReadClient
	writeStreams  chan valueproto.ValueStore_StreamWriteClient
	deleteStreams chan valueproto.ValueStore_StreamDeleteClient
}

// NewValueStore creates a ValueStore connection via grpc to the given address;
// note that Startup() will have been called in the returned store, so calling
// Startup() yourself is optional.
func NewValueStore(addr string, streams int, opts ...grpc.DialOption) (store.ValueStore, error) {
	v := &valueStore{
		addr: addr,
		opts: opts,
	}
	v.lookupStreams = make(chan valueproto.ValueStore_StreamLookupClient, streams)
	v.readStreams = make(chan valueproto.ValueStore_StreamReadClient, streams)
	v.writeStreams = make(chan valueproto.ValueStore_StreamWriteClient, streams)
	v.deleteStreams = make(chan valueproto.ValueStore_StreamDeleteClient, streams)
	return v, v.Startup()
}

func (v *valueStore) Startup() error {
	v.lock.Lock()
	defer v.lock.Unlock()
	if v.conn != nil {
		return nil
	}
	var err error
	v.conn, err = grpc.Dial(v.addr, v.opts...)
	if err != nil {
		v.conn = nil
		return err
	}
	v.client = valueproto.NewValueStoreClient(v.conn)
	for i := cap(v.lookupStreams); i > 0; i-- {
		v.lookupStreams <- nil
	}
	for i := cap(v.readStreams); i > 0; i-- {
		v.readStreams <- nil
	}
	for i := cap(v.writeStreams); i > 0; i-- {
		v.writeStreams <- nil
	}
	for i := cap(v.deleteStreams); i > 0; i-- {
		v.deleteStreams <- nil
	}
	return nil
}

func (v *valueStore) Shutdown() error {
	v.lock.Lock()
	defer v.lock.Unlock()
	if v.conn == nil {
		return nil
	}
	v.conn.Close()
	v.conn = nil
	v.client = nil
	for i := cap(v.lookupStreams); i > 0; i-- {
		<-v.lookupStreams
	}
	for i := cap(v.readStreams); i > 0; i-- {
		<-v.readStreams
	}
	for i := cap(v.writeStreams); i > 0; i-- {
		<-v.writeStreams
	}
	for i := cap(v.deleteStreams); i > 0; i-- {
		<-v.deleteStreams
	}
	return nil
}

func (v *valueStore) EnableWrites() error {
	return nil
}

func (v *valueStore) DisableWrites() error {
	// TODO: I suppose we could implement toggling writes from this client;
	// I'll leave that for later.
	return errors.New("cannot disable writes with this client at this time")
}

func (v *valueStore) Flush() error {
	// Nothing cached on this end, so nothing to flush.
	return nil
}

func (v *valueStore) AuditPass() error {
	return errors.New("audit passes not available with this client at this time")
}

func (v *valueStore) Stats(debug bool) (fmt.Stringer, error) {
	return noStats, nil
}

func (v *valueStore) ValueCap() (uint32, error) {
	// TODO: This should be a (cached) value from the server. Servers don't
	// change their value caps on the fly, so the cache can be kept until
	// disconnect.
	return 0xffffffff, nil
}

func (v *valueStore) Lookup(keyA, keyB uint64) (timestampmicro int64, length uint32, err error) {
	s := <-v.lookupStreams
	if s == nil {
		v.lock.Lock()
		s, err = v.client.StreamLookup(context.Background())
		v.lock.Unlock()
		if err != nil {
			v.lookupStreams <- nil
			return 0, 0, err
		}
	}
	req := &valueproto.LookupRequest{
		KeyA: keyA,
		KeyB: keyB,
	}
	if err = s.Send(req); err != nil {
		v.lookupStreams <- nil
		return 0, 0, err
	}
	res, err := s.Recv()
	if err != nil {
		v.lookupStreams <- nil
		return 0, 0, err
	}
	if res.Err != "" {
		err = proto.TranslateErrorString(res.Err)
	}
	v.lookupStreams <- s
	return res.TimestampMicro, res.Length, err
}

func (v *valueStore) Read(keyA, keyB uint64, value []byte) (timestampmicro int64, rvalue []byte, err error) {
	rvalue = value
	s := <-v.readStreams
	if s == nil {
		v.lock.Lock()
		s, err = v.client.StreamRead(context.Background())
		v.lock.Unlock()
		if err != nil {
			v.readStreams <- nil
			return 0, rvalue, err
		}
	}
	req := &valueproto.ReadRequest{
		KeyA: keyA,
		KeyB: keyB,
	}
	if err = s.Send(req); err != nil {
		v.readStreams <- nil
		return 0, rvalue, err
	}
	res, err := s.Recv()
	if err != nil {
		v.readStreams <- nil
		return 0, rvalue, err
	}
	rvalue = append(rvalue, res.Value...)
	if res.Err != "" {
		err = proto.TranslateErrorString(res.Err)
	}
	v.readStreams <- s
	return res.TimestampMicro, rvalue, err
}

func (v *valueStore) Write(keyA, keyB uint64, timestampmicro int64, value []byte) (oldtimestampmicro int64, err error) {
	s := <-v.writeStreams
	if s == nil {
		v.lock.Lock()
		s, err = v.client.StreamWrite(context.Background())
		v.lock.Unlock()
		if err != nil {
			v.writeStreams <- nil
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
		v.writeStreams <- nil
		return 0, err
	}
	res, err := s.Recv()
	if err != nil {
		v.writeStreams <- nil
		return 0, err
	}
	if res.Err != "" {
		err = proto.TranslateErrorString(res.Err)
	}
	v.writeStreams <- s
	return res.TimestampMicro, err
}

func (v *valueStore) Delete(keyA, keyB uint64, timestampmicro int64) (oldtimestampmicro int64, err error) {
	s := <-v.deleteStreams
	if s == nil {
		v.lock.Lock()
		s, err = v.client.StreamDelete(context.Background())
		v.lock.Unlock()
		if err != nil {
			v.deleteStreams <- nil
			return 0, err
		}
	}
	req := &valueproto.DeleteRequest{
		KeyA:           keyA,
		KeyB:           keyB,
		TimestampMicro: timestampmicro,
	}
	if err = s.Send(req); err != nil {
		v.deleteStreams <- nil
		return 0, err
	}
	res, err := s.Recv()
	if err != nil {
		v.deleteStreams <- nil
		return 0, err
	}
	if res.Err != "" {
		err = proto.TranslateErrorString(res.Err)
	}
	v.deleteStreams <- s
	return res.TimestampMicro, err
}
