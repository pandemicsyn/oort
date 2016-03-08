package api

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gholt/ring"
	"github.com/gholt/store"
	"golang.org/x/net/context"
)

type ReplGroupStore struct {
	logDebug                   func(string, ...interface{})
	logDebugOn                 bool
	addressIndex               int
	valueCap                   int
	concurrentRequestsPerStore int
	streamsPerStore            int
	failedConnectRetryDelay    int

	ringLock sync.RWMutex
	ring     ring.Ring

	storesLock sync.RWMutex
	stores     map[string]*replGroupStoreAndTicketChan
}

type replGroupStoreAndTicketChan struct {
	store      store.GroupStore
	ticketChan chan struct{}
}

func NewReplGroupStore(c *ReplGroupStoreConfig) *ReplGroupStore {
	cfg := resolveReplGroupStoreConfig(c)
	rs := &ReplGroupStore{
		logDebug:                   cfg.LogDebug,
		logDebugOn:                 cfg.LogDebug != nil,
		addressIndex:               cfg.AddressIndex,
		valueCap:                   int(cfg.ValueCap),
		concurrentRequestsPerStore: cfg.ConcurrentRequestsPerStore,
		streamsPerStore:            cfg.StreamsPerStore,
		failedConnectRetryDelay:    cfg.FailedConnectRetryDelay,
	}
	if rs.logDebug == nil {
		rs.logDebug = func(string, ...interface{}) {}
	}
	return rs
}

func (rs *ReplGroupStore) Ring() ring.Ring {
	rs.ringLock.RLock()
	r := rs.ring
	rs.ringLock.RUnlock()
	return r
}

func (rs *ReplGroupStore) SetRing(r ring.Ring) {
	rs.ringLock.Lock()
	rs.ring = r
	rs.ringLock.Unlock()
	var currentAddrs map[string]struct{}
	if r != nil {
		nodes := r.Nodes()
		currentAddrs = make(map[string]struct{}, len(nodes))
		for _, n := range nodes {
			currentAddrs[n.Address(rs.addressIndex)] = struct{}{}
		}
	}
	var shutdownAddrs []string
	rs.storesLock.RLock()
	for a := range rs.stores {
		if _, ok := currentAddrs[a]; !ok {
			shutdownAddrs = append(shutdownAddrs, a)
		}
	}
	rs.storesLock.RUnlock()
	if len(shutdownAddrs) > 0 {
		shutdownStores := make([]*replGroupStoreAndTicketChan, len(shutdownAddrs))
		rs.storesLock.Lock()
		for i, a := range shutdownAddrs {
			shutdownStores[i] = rs.stores[a]
			rs.stores[a] = nil
		}
		rs.storesLock.Unlock()
		for i, s := range shutdownStores {
			if err := s.store.Shutdown(context.Background()); err != nil {
				rs.logDebug("replGroupStore: error during shutdown of store %s: %s", shutdownAddrs[i], err)
			}
		}
	}
}

func (rs *ReplGroupStore) storesFor(ctx context.Context, keyA uint64) ([]*replGroupStoreAndTicketChan, error) {
	rs.ringLock.RLock()
	r := rs.ring
	rs.ringLock.RUnlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if r == nil {
		return nil, noRingErr
	}
	ns := r.ResponsibleNodes(uint32(keyA >> (64 - r.PartitionBitCount())))
	as := make([]string, len(ns))
	for i, n := range ns {
		as[i] = n.Address(rs.addressIndex)
	}
	ss := make([]*replGroupStoreAndTicketChan, len(ns))
	var someNil bool
	rs.storesLock.RLock()
	for i := len(ss) - 1; i >= 0; i-- {
		ss[i] = rs.stores[as[i]]
		if ss[i] == nil {
			someNil = true
		}
	}
	rs.storesLock.RUnlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if someNil {
		rs.storesLock.Lock()
		select {
		case <-ctx.Done():
			rs.storesLock.Unlock()
			return nil, ctx.Err()
		default:
		}
		for i := len(ss) - 1; i >= 0; i-- {
			if ss[i] == nil {
				ss[i] = rs.stores[as[i]]
				if ss[i] == nil {
					var err error
					tc := make(chan struct{}, rs.concurrentRequestsPerStore)
					for i := cap(tc); i > 0; i-- {
						tc <- struct{}{}
					}
					ss[i] = &replGroupStoreAndTicketChan{ticketChan: tc}
					// TODO: Right now, the following NewGroupStore calls its
					// Startup for you; but we'd like to undo that extra bit
					// and make Startup a separate call; otherwise the ctx
					// passed here is a little confusing.
					ss[i].store, err = NewGroupStore(ctx, as[i], rs.streamsPerStore)
					if err != nil {
						ss[i].store = errorGroupStore(fmt.Sprintf("could not create store for %s: %s", as[i], err))
						// Launch goroutine to clear out the error store after
						// some time so a retry will occur.
						go func(addr string) {
							time.Sleep(time.Duration(rs.failedConnectRetryDelay) * time.Second)
							rs.storesLock.Lock()
							s := rs.stores[addr]
							if s != nil {
								if _, ok := s.store.(errorGroupStore); ok {
									rs.stores[addr] = nil
								}
							}
							rs.storesLock.Unlock()
						}(as[i])
					}
					rs.stores[as[i]] = ss[i]
					select {
					case <-ctx.Done():
						rs.storesLock.Unlock()
						return nil, ctx.Err()
					default:
					}
				}
			}
		}
		rs.storesLock.Unlock()
	}
	return ss, nil
}

func (rs *ReplGroupStore) Startup(ctx context.Context) error {
	return nil
}

func (rs *ReplGroupStore) Shutdown(ctx context.Context) error {
	return nil
}

func (rs *ReplGroupStore) EnableWrites(ctx context.Context) error {
	return nil
}

func (rs *ReplGroupStore) DisableWrites(ctx context.Context) error {
	return errors.New("cannot disable writes with this client at this time")
}

func (rs *ReplGroupStore) Flush(ctx context.Context) error {
	return nil
}

func (rs *ReplGroupStore) AuditPass(ctx context.Context) error {
	return errors.New("audit passes not available with this client at this time")
}

func (rs *ReplGroupStore) Stats(ctx context.Context, debug bool) (fmt.Stringer, error) {
	return noStats, nil
}

func (rs *ReplGroupStore) ValueCap(ctx context.Context) (uint32, error) {
	return uint32(rs.valueCap), nil
}

func (rs *ReplGroupStore) Lookup(ctx context.Context, keyA, keyB uint64, childKeyA, childKeyB uint64) (int64, uint32, error) {
	type rettype struct {
		timestampMicro int64
		length         uint32
		err            ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		return 0, 0, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.timestampMicro, ret.length, err = s.store.Lookup(ctx, keyA, keyB, childKeyA, childKeyB)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var timestampMicro int64
	var length uint32
	var notFound bool
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.timestampMicro > timestampMicro {
			timestampMicro = ret.timestampMicro
			length = ret.length
			notFound = store.IsNotFound(ret.err)
		}
		if ret.err != nil {
			errs = append(errs, ret.err)
		}
	}
	if notFound {
		nferrs := make(ReplGroupStoreErrorNotFound, len(errs))
		for i, v := range errs {
			nferrs[i] = v
		}
		return timestampMicro, length, nferrs
	}
	if len(errs) < len(stores) {
		for _, err := range errs {
			rs.logDebug("replGroupStore: error during lookup: %s", err)
		}
		errs = nil
	}
	return timestampMicro, length, errs
}

func (rs *ReplGroupStore) Read(ctx context.Context, keyA uint64, keyB uint64, childKeyA, childKeyB uint64, value []byte) (int64, []byte, error) {
	type rettype struct {
		timestampMicro int64
		value          []byte
		err            ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		return 0, nil, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.timestampMicro, ret.value, err = s.store.Read(ctx, keyA, keyB, childKeyA, childKeyB, nil)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var timestampMicro int64
	var rvalue []byte
	var notFound bool
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.timestampMicro > timestampMicro {
			timestampMicro = ret.timestampMicro
			rvalue = ret.value
			notFound = store.IsNotFound(ret.err)
		}
		if ret.err != nil {
			errs = append(errs, ret.err)
		}
	}
	if value != nil && rvalue != nil {
		rvalue = append(value, rvalue...)
	}
	if notFound {
		nferrs := make(ReplGroupStoreErrorNotFound, len(errs))
		for i, v := range errs {
			nferrs[i] = v
		}
		return timestampMicro, rvalue, nferrs
	}
	if len(errs) < len(stores) {
		for _, err := range errs {
			rs.logDebug("replGroupStore: error during read: %s", err)
		}
		errs = nil
	}
	return timestampMicro, rvalue, errs
}

func (rs *ReplGroupStore) Write(ctx context.Context, keyA uint64, keyB uint64, childKeyA, childKeyB uint64, timestampMicro int64, value []byte) (int64, error) {
	if len(value) > rs.valueCap {
		return 0, fmt.Errorf("value length of %d > %d", len(value), rs.valueCap)
	}
	type rettype struct {
		oldTimestampMicro int64
		err               ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		return 0, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.oldTimestampMicro, err = s.store.Write(ctx, keyA, keyB, childKeyA, childKeyB, timestampMicro, value)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var oldTimestampMicro int64
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if ret.oldTimestampMicro > oldTimestampMicro {
			oldTimestampMicro = ret.oldTimestampMicro
		}
	}
	if len(errs) < (len(stores)+1)/2 {
		for _, err := range errs {
			rs.logDebug("replGroupStore: error during write: %s", err)
		}
		errs = nil
	}
	return oldTimestampMicro, errs
}

func (rs *ReplGroupStore) Delete(ctx context.Context, keyA uint64, keyB uint64, childKeyA, childKeyB uint64, timestampMicro int64) (int64, error) {
	type rettype struct {
		oldTimestampMicro int64
		err               ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		return 0, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.oldTimestampMicro, err = s.store.Delete(ctx, keyA, keyB, childKeyA, childKeyB, timestampMicro)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var oldTimestampMicro int64
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if ret.oldTimestampMicro > oldTimestampMicro {
			oldTimestampMicro = ret.oldTimestampMicro
		}
	}
	if len(errs) < (len(stores)+1)/2 {
		for _, err := range errs {
			rs.logDebug("replGroupStore: error during delete: %s", err)
		}
		errs = nil
	}
	return oldTimestampMicro, errs
}

func (rs *ReplGroupStore) LookupGroup(ctx context.Context, parentKeyA, parentKeyB uint64) ([]store.LookupGroupItem, error) {
	type rettype struct {
		items []store.LookupGroupItem
		err   ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, parentKeyA)
	if err != nil {
		return nil, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.items, err = s.store.LookupGroup(ctx, parentKeyA, parentKeyB)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var items []store.LookupGroupItem
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if len(ret.items) > len(items) {
			items = ret.items
		}
	}
	if len(errs) == len(stores) {
		return items, errs
	} else {
		for _, err := range errs {
			rs.logDebug("replGroupStore: error during lookup group: %s", err)
		}
	}
	return items, nil
}

func (rs *ReplGroupStore) ReadGroup(ctx context.Context, parentKeyA, parentKeyB uint64) ([]store.ReadGroupItem, error) {
	type rettype struct {
		items []store.ReadGroupItem
		err   ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, parentKeyA)
	if err != nil {
		return nil, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.items, err = s.store.ReadGroup(ctx, parentKeyA, parentKeyB)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var items []store.ReadGroupItem
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if len(ret.items) > len(items) {
			items = ret.items
		}
	}
	if len(errs) == len(stores) {
		return items, errs
	} else {
		for _, err := range errs {
			rs.logDebug("replGroupStore: error during read group: %s", err)
		}
	}
	return items, nil
}

type ReplGroupStoreError interface {
	error
	Store() store.GroupStore
	Err() error
}

type ReplGroupStoreErrorSlice []ReplGroupStoreError

func (es ReplGroupStoreErrorSlice) Error() string {
	if len(es) <= 0 {
		return "unknown error"
	} else if len(es) == 1 {
		return es[0].Error()
	}
	return fmt.Sprintf("%d errors, first is: %s", len(es), es[0])
}

type ReplGroupStoreErrorNotFound ReplGroupStoreErrorSlice

func (e ReplGroupStoreErrorNotFound) Error() string {
	if len(e) <= 0 {
		return "unknown error"
	} else if len(e) == 1 {
		return e[0].Error()
	}
	return fmt.Sprintf("%d errors, first is: %s", len(e), e[0])
}

func (e ReplGroupStoreErrorNotFound) ErrorNotFound() string {
	return e.Error()
}

type replGroupStoreError struct {
	store store.GroupStore
	err   error
}

func (e *replGroupStoreError) Error() string {
	if e.err == nil {
		return "unknown error"
	}
	return e.err.Error()
}

func (e *replGroupStoreError) Store() store.GroupStore {
	return e.store
}

func (e *replGroupStoreError) Err() error {
	return e.err
}
