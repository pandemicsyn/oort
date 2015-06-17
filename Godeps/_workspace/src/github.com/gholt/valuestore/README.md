# ValueStore
## Development Repository

**This is a very early prototype of an idea we may come back to later. I just
wanted to get it recorded in case it is useful later.**

Package valuestore provides a disk-backed data structure for use in storing
[]byte values referenced by 128 bit keys with options for replication.

It can handle billions of keys (as memory allows) and full concurrent access
across many cores. All location information about each key is stored in memory
for speed, but values are stored on disk with the exception of recently written
data being buffered first and batched to disk later.

This has been written with SSDs in mind, but spinning drives should work as
well; though storing valuestoc files (Table Of Contents, key location
information) on a separate disk from values files is recommended in that case.

Each key is two 64bit values, known as keyA and keyB uint64 values. These are
usually created by a hashing function of the key name, but that duty is left
outside this package.

Each modification is recorded with an int64 timestamp that is number of
microseconds since the Unix epoch (see
github.com/gholt/brimtime.TimeToUnixMicro). With a write and delete for the
exact same timestamp, the delete wins. This allows a delete to be issued for a
specific write without fear of deleting any newer write.

Internally, each modification is stored with a uint64 timestamp that is
equivalent to (brimtime.TimeToUnixMicro(time.Now())<<8) with the lowest 8
bits used to indicate deletions and other bookkeeping items. This means that
the allowable time range is 1970-01-01 00:00:00 +0000 UTC (+1 microsecond
because all zeroes indicates a missing item) to 4253-05-31 22:20:37.927935
+0000 UTC. There are constants TIMESTAMPMICRO_MIN and TIMESTAMPMICRO_MAX
available for bounding usage.

There are background tasks for:

* TombstoneDiscard: This will discard older tombstones (deletion markers).
Tombstones are kept for OptTombstoneAge seconds and are used to ensure a
replicated older value doesn't resurrect a deleted value. But, keeping all
tombstones for all time is a waste of resources, so they are discarded over
time. OptTombstoneAge controls how long they should be kept and should be
set to an amount greater than several replication passes.

* PullReplication: This will continually send out pull replication requests
for all the partitions the ValueStore is responsible for, as determined by
the OptMsgRing. The other responsible parties will respond to these requests
with data they have that was missing from the pull replication request.
Bloom filters are used to reduce bandwidth which has the downside that a
very small percentage of items may be missed each pass. A moving salt is
used with each bloom filter so that after a few passes there is an
exceptionally high probability that all items will be accounted for.

* PushReplication: This will continually send out any data for any
partitions the ValueStore is *not* responsible for, as determined by the
OptMsgRing. The responsible parties will respond to these requests with
acknowledgements of the data they received, allowing the requester to
discard the out of place data.

[API Documentation](http://godoc.org/github.com/gholt/valuestore)

This is the latest development area for the package.  
Eventually a stable version of the package will be established but, for now,
all things about this package are subject to change.

> Copyright See AUTHORS. All rights reserved.  
> Use of this source code is governed by a BSD-style  
> license that can be found in the LICENSE file.
