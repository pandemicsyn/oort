package api

import (
	"errors"
	"fmt"
	"sync"

	"github.com/gholt/store"
	"github.com/pandemicsyn/oort/api/proto"
	pb "github.com/pandemicsyn/oort/api/valueproto"
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
	client        pb.ValueStoreClient
	lookupStreams chan pb.ValueStore_StreamLookupClient
	readStreams   chan pb.ValueStore_StreamReadClient
	writeStreams  chan pb.ValueStore_StreamWriteClient
	deleteStreams chan pb.ValueStore_StreamDeleteClient
}

// NewValueStore creates a ValueStore connection via grpc to the given
// address.
func NewValueStore(addr string, streams int, opts ...grpc.DialOption) (store.ValueStore, error) {
	str := &valueStore{
		addr: addr,
		opts: opts,
	}
	str.lookupStreams = make(chan pb.ValueStore_StreamLookupClient, streams)
	str.readStreams = make(chan pb.ValueStore_StreamReadClient, streams)
	str.writeStreams = make(chan pb.ValueStore_StreamWriteClient, streams)
	str.deleteStreams = make(chan pb.ValueStore_StreamDeleteClient, streams)

	for i := cap(str.lookupStreams); i > 0; i-- {
		str.lookupStreams <- nil
	}
	for i := cap(str.readStreams); i > 0; i-- {
		str.readStreams <- nil
	}
	for i := cap(str.writeStreams); i > 0; i-- {
		str.writeStreams <- nil
	}
	for i := cap(str.deleteStreams); i > 0; i-- {
		str.deleteStreams <- nil
	}

	return str, nil
}

func (str *valueStore) Startup(ctx context.Context) error {
	str.lock.Lock()
	err := str.startup()
	str.lock.Unlock()
	return err
}

func (str *valueStore) startup() error {
	if str.conn != nil {
		return nil
	}
	var err error
	str.conn, err = grpc.Dial(str.addr, str.opts...)
	if err != nil {
		str.conn = nil
		return err
	}
	str.client = pb.NewValueStoreClient(str.conn)
	return nil
}

func (str *valueStore) Shutdown(ctx context.Context) error {
	str.lock.Lock()
	defer str.lock.Unlock()
	if str.conn == nil {
		return nil
	}
	str.conn.Close()
	str.conn = nil
	str.client = nil
	for i := cap(str.lookupStreams); i > 0; i-- {
		<-str.lookupStreams
		str.lookupStreams <- nil
	}
	for i := cap(str.readStreams); i > 0; i-- {
		<-str.readStreams
		str.readStreams <- nil
	}
	for i := cap(str.writeStreams); i > 0; i-- {
		<-str.writeStreams
		str.writeStreams <- nil
	}
	for i := cap(str.deleteStreams); i > 0; i-- {
		<-str.deleteStreams
		str.deleteStreams <- nil
	}
	return nil
}

func (str *valueStore) EnableWrites(ctx context.Context) error {
	return nil
}

func (str *valueStore) DisableWrites(ctx context.Context) error {
	// TODO: I suppose we could implement toggling writes from this client;
	// I'll leave that for later.
	return errors.New("cannot disable writes with this client at this time")
}

func (str *valueStore) Flush(ctx context.Context) error {
	// Nothing cached on this end, so nothing to flush.
	return nil
}

func (str *valueStore) AuditPass(ctx context.Context) error {
	return errors.New("audit passes not available with this client at this time")
}

func (str *valueStore) Stats(ctx context.Context, debug bool) (fmt.Stringer, error) {
	return noStats, nil
}

func (str *valueStore) ValueCap(ctx context.Context) (uint32, error) {
	// TODO: This should be a (cached) value from the server. Servers don't
	// change their value caps on the fly, so the cache can be kept until
	// disconnect.
	return 0xffffffff, nil
}

func (str *valueStore) Lookup(ctx context.Context, keyA, keyB uint64) (timestampmicro int64, length uint32, err error) {
	// TODO: Pay attention to ctx.
	var s pb.ValueStore_StreamLookupClient
	select {
	case s = <-str.lookupStreams:
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
	if s == nil {
		str.lock.Lock()
		select {
		case <-ctx.Done():
			str.lock.Unlock()
			str.lookupStreams <- nil
			return 0, 0, ctx.Err()
		default:
		}
		if str.client == nil {
			if err := str.startup(); err != nil {
				str.lock.Unlock()
				str.lookupStreams <- nil
				return 0, 0, err
			}
		}
		s, err = str.client.StreamLookup(context.Background())
		str.lock.Unlock()
		if err != nil {
			str.lookupStreams <- nil
			return 0, 0, err
		}
	}
	req := &pb.LookupRequest{
		KeyA: keyA,
		KeyB: keyB,
	}
	if err = s.Send(req); err != nil {
		str.lookupStreams <- nil
		return 0, 0, err
	}
	res, err := s.Recv()
	if err != nil {
		str.lookupStreams <- nil
		return 0, 0, err
	}
	if res.Err != "" {
		err = proto.TranslateErrorString(res.Err)
	}
	str.lookupStreams <- s
	return res.TimestampMicro, res.Length, err
}

func (str *valueStore) Read(ctx context.Context, keyA, keyB uint64, value []byte) (timestampmicro int64, rvalue []byte, err error) {
	// TODO: Pay attention to ctx.
	rvalue = value
	s := <-str.readStreams
	if s == nil {
		str.lock.Lock()
		if str.client == nil {
			if err := str.startup(); err != nil {
				str.lock.Unlock()
				str.readStreams <- nil
				return 0, rvalue, err
			}
		}
		s, err = str.client.StreamRead(context.Background())
		str.lock.Unlock()
		if err != nil {
			str.readStreams <- nil
			return 0, rvalue, err
		}
	}
	req := &pb.ReadRequest{
		KeyA: keyA,
		KeyB: keyB,
	}
	if err = s.Send(req); err != nil {
		str.readStreams <- nil
		return 0, rvalue, err
	}
	res, err := s.Recv()
	if err != nil {
		str.readStreams <- nil
		return 0, rvalue, err
	}
	rvalue = append(rvalue, res.Value...)
	if res.Err != "" {
		err = proto.TranslateErrorString(res.Err)
	}
	str.readStreams <- s
	return res.TimestampMicro, rvalue, err
}

func (str *valueStore) Write(ctx context.Context, keyA, keyB uint64, timestampmicro int64, value []byte) (oldtimestampmicro int64, err error) {
	// TODO: Pay attention to ctx.
	s := <-str.writeStreams
	if s == nil {
		str.lock.Lock()
		if str.client == nil {
			if err := str.startup(); err != nil {
				str.lock.Unlock()
				str.writeStreams <- nil
				return 0, err
			}
		}
		s, err = str.client.StreamWrite(context.Background())
		str.lock.Unlock()
		if err != nil {
			str.writeStreams <- nil
			return 0, err
		}
	}
	req := &pb.WriteRequest{
		KeyA: keyA,
		KeyB: keyB,

		TimestampMicro: timestampmicro,
		Value:          value,
	}
	if err = s.Send(req); err != nil {
		str.writeStreams <- nil
		return 0, err
	}
	res, err := s.Recv()
	if err != nil {
		str.writeStreams <- nil
		return 0, err
	}
	if res.Err != "" {
		err = proto.TranslateErrorString(res.Err)
	}
	str.writeStreams <- s
	return res.TimestampMicro, err
}

func (str *valueStore) Delete(ctx context.Context, keyA, keyB uint64, timestampmicro int64) (oldtimestampmicro int64, err error) {
	// TODO: Pay attention to ctx.
	s := <-str.deleteStreams
	if s == nil {
		str.lock.Lock()
		if str.client == nil {
			if err := str.startup(); err != nil {
				str.lock.Unlock()
				str.deleteStreams <- nil
				return 0, err
			}
		}
		s, err = str.client.StreamDelete(context.Background())
		str.lock.Unlock()
		if err != nil {
			str.deleteStreams <- nil
			return 0, err
		}
	}
	req := &pb.DeleteRequest{
		KeyA: keyA,
		KeyB: keyB,

		TimestampMicro: timestampmicro,
	}
	if err = s.Send(req); err != nil {
		str.deleteStreams <- nil
		return 0, err
	}
	res, err := s.Recv()
	if err != nil {
		str.deleteStreams <- nil
		return 0, err
	}
	if res.Err != "" {
		err = proto.TranslateErrorString(res.Err)
	}
	str.deleteStreams <- s
	return res.TimestampMicro, err
}
