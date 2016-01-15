// Package store provides a disk-backed data structure for use in storing
// []byte values referenced by 128 bit keys with options for replication.
//
// It can handle billions of keys (as memory allows) and full concurrent access
// across many cores. All location information about each key is stored in
// memory for speed, but values are stored on disk with the exception of
// recently written data being buffered first and batched to disk later.
//
// This has been written with SSDs in mind, but spinning drives should work
// also; though storing toc files (Table Of Contents, key location information)
// on a separate disk from values files is recommended in that case.
//
// Each key is two 64bit values, known as keyA and keyB uint64 values. These
// are usually created by a hashing function of the key name, but that duty is
// left outside this package.
//
// Each modification is recorded with an int64 timestamp that is the number of
// microseconds since the Unix epoch (see
// github.com/gholt/brimtime.TimeToUnixMicro). With a write and delete for the
// exact same timestamp, the delete wins. This allows a delete to be issued for
// a specific write without fear of deleting any newer write.
//
// Internally, each modification is stored with a uint64 timestamp that is
// equivalent to (brimtime.TimeToUnixMicro(time.Now())<<8) with the lowest 8
// bits used to indicate deletions and other bookkeeping items. This means that
// the allowable time range is 1970-01-01 00:00:00 +0000 UTC (+1 microsecond
// because all zeroes indicates a missing item) to 4253-05-31 22:20:37.927935
// +0000 UTC. There are constants TIMESTAMPMICRO_MIN and TIMESTAMPMICRO_MAX
// available for bounding usage.
//
// There are background tasks for:
//
// * TombstoneDiscard: This will discard older tombstones (deletion markers).
// Tombstones are kept for Config.TombstoneAge seconds and are used to ensure a
// replicated older value doesn't resurrect a deleted value. But, keeping all
// tombstones for all time is a waste of resources, so they are discarded over
// time. Config.TombstoneAge controls how long they should be kept and should
// be set to an amount greater than several replication passes.
//
// * PullReplication: This will continually send out pull replication requests
// for all the partitions the ValueStore is responsible for, as determined by
// the Config.MsgRing. The other responsible parties will respond to these
// requests with data they have that was missing from the pull replication
// request. Bloom filters are used to reduce bandwidth which has the downside
// that a very small percentage of items may be missed each pass. A moving salt
// is used with each bloom filter so that after a few passes there is an
// exceptionally high probability that all items will be accounted for.
//
// * PushReplication: This will continually send out any data for any
// partitions the ValueStore is *not* responsible for, as determined by the
// Config.MsgRing. The responsible parties will respond to these requests with
// acknowledgements of the data they received, allowing the requester to
// discard the out of place data.
//
// * Compaction: TODO description.
//
// * Audit: This will verify the data on disk has not been corrupted. It will
// slowly read data over time and validate checksums. If it finds issues, it
// will try to remove affected entries the in-memory location map so that
// replication from other stores will send the information they have and the
// values will get re-stored locally. In cases where the affected entries
// cannot be determined, it will make a callback requesting the store be
// shutdown and restarted; this restart will result in the affected keys being
// missing and therefore replicated in by other stores.
//
// Note that if the disk gets filled past a configurable threshold, any
// external writes other than deletes will result in error. Internal writes
// such as compaction and removing successfully push-replicated data will
// continue.
//
// There is also a modified form of ValueStore called GroupStore that expands
// the primary key to two 128 bit keys and offers a Lookup methods which
// retrieves all matching items for the first key.
package store

// got is at https://github.com/gholt/got
//go:generate got store.got valuestore_GEN_.go TT=VALUE T=Value t=value
//go:generate got store.got groupstore_GEN_.go TT=GROUP T=Group t=group
//go:generate got store_test.got valuestore_GEN_test.go TT=VALUE T=Value t=value
//go:generate got store_test.got groupstore_GEN_test.go TT=GROUP T=Group t=group
//go:generate got config.got valueconfig_GEN_.go TT=VALUE T=Value t=value
//go:generate got config.got groupconfig_GEN_.go TT=GROUP T=Group t=group
//go:generate got memblock.got valuememblock_GEN_.go TT=VALUE T=Value t=value
//go:generate got memblock.got groupmemblock_GEN_.go TT=GROUP T=Group t=group
//go:generate got memblock_test.got valuememblock_GEN_test.go TT=VALUE T=Value t=value
//go:generate got memblock_test.got groupmemblock_GEN_test.go TT=GROUP T=Group t=group
//go:generate got storefile.got valuestorefile_GEN_.go TT=VALUE T=Value t=value
//go:generate got storefile.got groupstorefile_GEN_.go TT=GROUP T=Group t=group
//go:generate got storefile_test.got valuestorefile_GEN_test.go TT=VALUE T=Value t=value
//go:generate got storefile_test.got groupstorefile_GEN_test.go TT=GROUP T=Group t=group
//go:generate got bulkset.got valuebulkset_GEN_.go TT=VALUE T=Value t=value
//go:generate got bulkset.got groupbulkset_GEN_.go TT=GROUP T=Group t=group
//go:generate got bulkset_test.got valuebulkset_GEN_test.go TT=VALUE T=Value t=value
//go:generate got bulkset_test.got groupbulkset_GEN_test.go TT=GROUP T=Group t=group
//go:generate got bulksetack.got valuebulksetack_GEN_.go TT=VALUE T=Value t=value
//go:generate got bulksetack.got groupbulksetack_GEN_.go TT=GROUP T=Group t=group
//go:generate got bulksetack_test.got valuebulksetack_GEN_test.go TT=VALUE T=Value t=value
//go:generate got bulksetack_test.got groupbulksetack_GEN_test.go TT=GROUP T=Group t=group
//go:generate got pullreplication.got valuepullreplication_GEN_.go TT=VALUE T=Value t=value
//go:generate got pullreplication.got grouppullreplication_GEN_.go TT=GROUP T=Group t=group
//go:generate got pullreplication_test.got valuepullreplication_GEN_test.go TT=VALUE T=Value t=value
//go:generate got pullreplication_test.got grouppullreplication_GEN_test.go TT=GROUP T=Group t=group
//go:generate got ktbloomfilter.got valuektbloomfilter_GEN_.go TT=VALUE T=Value t=value
//go:generate got ktbloomfilter.got groupktbloomfilter_GEN_.go TT=GROUP T=Group t=group
//go:generate got ktbloomfilter_test.got valuektbloomfilter_GEN_test.go TT=VALUE T=Value t=value
//go:generate got ktbloomfilter_test.got groupktbloomfilter_GEN_test.go TT=GROUP T=Group t=group
//go:generate got pushreplication.got valuepushreplication_GEN_.go TT=VALUE T=Value t=value
//go:generate got pushreplication.got grouppushreplication_GEN_.go TT=GROUP T=Group t=group
//go:generate got tombstonediscard.got valuetombstonediscard_GEN_.go TT=VALUE T=Value t=value
//go:generate got tombstonediscard.got grouptombstonediscard_GEN_.go TT=GROUP T=Group t=group
//go:generate got compaction.got valuecompaction_GEN_.go TT=VALUE T=Value t=value
//go:generate got compaction.got groupcompaction_GEN_.go TT=GROUP T=Group t=group
//go:generate got audit.got valueaudit_GEN_.go TT=VALUE T=Value t=value
//go:generate got audit.got groupaudit_GEN_.go TT=GROUP T=Group t=group
//go:generate got diskwatcher.got valuediskwatcher_GEN_.go TT=VALUE T=Value t=value
//go:generate got diskwatcher.got groupdiskwatcher_GEN_.go TT=GROUP T=Group t=group
//go:generate got flusher.got valueflusher_GEN_.go TT=VALUE T=Value t=value
//go:generate got flusher.got groupflusher_GEN_.go TT=GROUP T=Group t=group
//go:generate got stats.got valuestats_GEN_.go TT=VALUE T=Value t=value
//go:generate got stats.got groupstats_GEN_.go TT=GROUP T=Group t=group

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
)

const (
	_TSB_UTIL_BITS = 8
	_TSB_INACTIVE  = 0xfe
	_TSB_DELETION  = 0x80
	// _TSB_COMPACTION_REWRITE indicates an item is being or has been rewritten
	// as part of compaction. Note that if this bit somehow ends up persisted,
	// it won't be considered an inactive marker since it's outside the
	// _TSB_INACTIVE mask.
	_TSB_COMPACTION_REWRITE = 0x01
	// _TSB_LOCAL_REMOVAL indicates an item to be removed locally due to push
	// replication (local store wasn't considered responsible for the item
	// according to the ring) or a deletion marker expiration. An item marked
	// for local removal will be retained in memory until the local removal
	// marker is written to disk.
	_TSB_LOCAL_REMOVAL = 0x02
)

const (
	TIMESTAMPMICRO_MIN = int64(uint64(1) << _TSB_UTIL_BITS)
	TIMESTAMPMICRO_MAX = int64(uint64(math.MaxUint64) >> _TSB_UTIL_BITS)
)

var ErrNotFound error = errors.New("not found")
var ErrDisabled error = errors.New("disabled")

var toss []byte = make([]byte, 65536)

func osOpenReadSeeker(name string) (io.ReadSeeker, error) {
	return os.Open(name)
}

func osOpenWriteSeeker(name string) (io.WriteSeeker, error) {
	return os.OpenFile(name, os.O_RDWR, 0666)
}

func osCreateWriteCloser(name string) (io.WriteCloser, error) {
	return os.Create(name)
}

type LogFunc func(format string, v ...interface{})

type bgNotificationAction int

const (
	_BG_PASS bgNotificationAction = iota
	_BG_DISABLE
)

type bgNotification struct {
	action   bgNotificationAction
	doneChan chan struct{}
}

// Store is an interface for a disk-backed data structure that stores
// []byte values referenced by keys with options for replication.
//
// For documentation on each of these functions, see the DefaultValueStore and
// DefaultGroupStore.
type Store interface {
	EnableAll()
	DisableAll()
	DisableAllBackground()
	EnableTombstoneDiscard()
	DisableTombstoneDiscard()
	TombstoneDiscardPass()
	EnableCompaction()
	DisableCompaction()
	CompactionPass()
	EnableAudit()
	DisableAudit()
	AuditPass()
	EnableOutPullReplication()
	DisableOutPullReplication()
	OutPullReplicationPass()
	EnableOutPushReplication()
	DisableOutPushReplication()
	OutPushReplicationPass()
	EnableWrites()
	DisableWrites()
	Flush()
	Stats(debug bool) fmt.Stringer
	ValueCap() uint32
}

// ValueStore is an interface for a disk-backed data structure that stores
// []byte values referenced by 128 bit keys with options for replication.
//
// For documentation on each of these functions, see the DefaultValueStore.
type ValueStore interface {
	Store
	Lookup(keyA uint64, keyB uint64) (int64, uint32, error)
	Read(keyA uint64, keyB uint64, value []byte) (int64, []byte, error)
	Write(keyA uint64, keyB uint64, timestamp int64, value []byte) (int64, error)
	Delete(keyA uint64, keyB uint64, timestamp int64) (int64, error)
}

// GroupStore is an interface for a disk-backed data structure that stores
// []byte values referenced by 128 bit key pairs with options for replication.
//
// For documentation on each of these functions, see the DefaultGroupStore.
type GroupStore interface {
	Store
	Lookup(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64) (int64, uint32, error)
	LookupGroup(keyA uint64, keyB uint64) []LookupGroupItem
	Read(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64, value []byte) (int64, []byte, error)
	Write(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64, timestamp int64, value []byte) (int64, error)
	Delete(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64, timestamp int64) (int64, error)
}

func closeIfCloser(thing interface{}) error {
	closer, ok := thing.(io.Closer)
	if ok {
		return closer.Close()
	}
	return nil
}
