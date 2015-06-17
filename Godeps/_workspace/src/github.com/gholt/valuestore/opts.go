package valuestore

import (
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/gholt/ring"
	"github.com/gholt/valuelocmap"
)

type config struct {
	logCritical                *log.Logger
	logError                   *log.Logger
	logWarning                 *log.Logger
	logInfo                    *log.Logger
	logDebug                   *log.Logger
	rand                       *rand.Rand
	path                       string
	pathtoc                    string
	vlm                        valuelocmap.ValueLocMap
	workers                    int
	tombstoneDiscardInterval   int
	outPullReplicationWorkers  int
	outPullReplicationInterval int
	outPushReplicationWorkers  int
	outPushReplicationInterval int
	maxValueSize               int
	pageSize                   int
	minValueAlloc              int
	writePagesPerWorker        int
	tombstoneAge               int
	valuesFileSize             int
	valuesFileReaders          int
	checksumInterval           int
	msgRing                    ring.MsgRing
	replicationIgnoreRecent    int
	compactionInterval         int
	compactionThreshold        float64
	compactionAgeThreshold     int
	compactionWorkers          int
}

func resolveConfig(opts ...func(*config)) *config {
	cfg := &config{}
	cfg.path = os.Getenv("VALUESTORE_PATH")
	cfg.pathtoc = os.Getenv("VALUESTORE_PATHTOC")
	cfg.workers = runtime.GOMAXPROCS(0)
	if env := os.Getenv("VALUESTORE_WORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.workers = val
		}
	}
	cfg.tombstoneDiscardInterval = 60
	if env := os.Getenv("VALUESTORE_TOMBSTONEDISCARDINTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.tombstoneDiscardInterval = val
		}
	}
	cfg.outPullReplicationWorkers = cfg.workers
	if env := os.Getenv("VALUESTORE_OUTPULLREPLICATIONWORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.outPullReplicationWorkers = val
		}
	}
	cfg.outPullReplicationInterval = 60
	if env := os.Getenv("VALUESTORE_OUTPULLREPLICATIONINTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.outPullReplicationInterval = val
		}
	}
	cfg.outPushReplicationWorkers = cfg.workers
	if env := os.Getenv("VALUESTORE_OUTPUSHREPLICATIONWORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.outPushReplicationWorkers = val
		}
	}
	cfg.outPushReplicationInterval = 60
	if env := os.Getenv("VALUESTORE_OUTPUSHREPLICATIONINTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.outPushReplicationInterval = val
		}
	}
	cfg.maxValueSize = 4 * 1024 * 1024
	if env := os.Getenv("VALUESTORE_MAXVALUESIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.maxValueSize = val
		}
	}
	cfg.pageSize = 4 * 1024 * 1024
	if env := os.Getenv("VALUESTORE_PAGESIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.pageSize = val
		}
	}
	cfg.writePagesPerWorker = 3
	if env := os.Getenv("VALUESTORE_WRITEPAGESPERWORKER"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.writePagesPerWorker = val
		}
	}
	cfg.tombstoneAge = 4 * 60 * 60
	if env := os.Getenv("VALUESTORE_TOMBSTONEAGE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.tombstoneAge = val
		}
	}
	cfg.valuesFileSize = math.MaxUint32
	if env := os.Getenv("VALUESTORE_VALUESFILESIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.valuesFileSize = val
		}
	}
	cfg.valuesFileReaders = cfg.workers
	if env := os.Getenv("VALUESTORE_VALUESFILEREADERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.valuesFileReaders = val
		}
	}
	cfg.checksumInterval = 65532
	if env := os.Getenv("VALUESTORE_CHECKSUMINTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.checksumInterval = val
		}
	}
	cfg.replicationIgnoreRecent = 60
	if env := os.Getenv("VALUESTORE_REPLICATIONIGNORERECENT"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.replicationIgnoreRecent = val
		}
	}
	cfg.compactionInterval = 300
	if env := os.Getenv("VALUESTORE_COMPACTIONINTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.compactionInterval = val
		}
	}
	cfg.compactionThreshold = 0.10
	if env := os.Getenv("VALUESTORE_COMPACTIONTHRESHOLD"); env != "" {
		if val, err := strconv.ParseFloat(env, 64); err == nil {
			cfg.compactionThreshold = val
		}
	}
	cfg.compactionAgeThreshold = 300
	if env := os.Getenv("VALUESTORE_COMPACTIONAGETHRESHOLD"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.compactionAgeThreshold = val
		}
	}
	cfg.compactionWorkers = 1
	if env := os.Getenv("VALUESTORE_COMPACTIONWORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.compactionWorkers = val
		}
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.logCritical == nil {
		cfg.logCritical = log.New(os.Stderr, "ValueStore ", log.LstdFlags)
	}
	if cfg.logError == nil {
		cfg.logError = log.New(os.Stderr, "ValueStore ", log.LstdFlags)
	}
	if cfg.logWarning == nil {
		cfg.logWarning = log.New(os.Stderr, "ValueStore ", log.LstdFlags)
	}
	if cfg.logInfo == nil {
		cfg.logInfo = log.New(os.Stdout, "ValueStore ", log.LstdFlags)
	}
	if cfg.rand == nil {
		cfg.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	if cfg.path == "" {
		cfg.path = "."
	}
	if cfg.pathtoc == "" {
		cfg.pathtoc = cfg.path
	}
	if cfg.workers < 1 {
		cfg.workers = 1
	}
	if cfg.tombstoneDiscardInterval < 1 {
		cfg.tombstoneDiscardInterval = 1
	}
	if cfg.outPullReplicationWorkers < 1 {
		cfg.outPullReplicationWorkers = 1
	}
	if cfg.outPullReplicationInterval < 1 {
		cfg.outPullReplicationInterval = 1
	}
	if cfg.outPushReplicationWorkers < 1 {
		cfg.outPushReplicationWorkers = 1
	}
	if cfg.outPushReplicationInterval < 1 {
		cfg.outPushReplicationInterval = 1
	}
	if cfg.maxValueSize < 0 {
		cfg.maxValueSize = 0
	}
	if cfg.maxValueSize > math.MaxUint32 {
		cfg.maxValueSize = math.MaxUint32
	}
	if cfg.checksumInterval < 1 {
		cfg.checksumInterval = 1
	}
	// Ensure each page will have at least checksumInterval worth of data in it
	// so that each page written will at least flush the previous page's data.
	if cfg.pageSize < cfg.maxValueSize+cfg.checksumInterval {
		cfg.pageSize = cfg.maxValueSize + cfg.checksumInterval
	}
	// Absolute minimum: timestampnano leader plus at least one TOC entry
	if cfg.pageSize < 40 {
		cfg.pageSize = 40
	}
	// The max is MaxUint32-1 because we use MaxUint32 to indicate push
	// replication local removal.
	if cfg.pageSize > math.MaxUint32-1 {
		cfg.pageSize = math.MaxUint32 - 1
	}
	// Ensure a full TOC page will have an associated data page of at least
	// checksumInterval in size, again so that each page written will at least
	// flush the previous page's data.
	cfg.minValueAlloc = cfg.checksumInterval/(cfg.pageSize/32+1) + 1
	if cfg.writePagesPerWorker < 2 {
		cfg.writePagesPerWorker = 2
	}
	if cfg.tombstoneAge < 0 {
		cfg.tombstoneAge = 0
	}
	if cfg.valuesFileSize < 48+cfg.maxValueSize { // header value trailer
		cfg.valuesFileSize = 48 + cfg.maxValueSize
	}
	if cfg.valuesFileSize > math.MaxUint32 {
		cfg.valuesFileSize = math.MaxUint32
	}
	if cfg.valuesFileReaders < 1 {
		cfg.valuesFileReaders = 1
	}
	if cfg.replicationIgnoreRecent < 0 {
		cfg.replicationIgnoreRecent = 0
	}
	if cfg.compactionInterval < 1 {
		cfg.compactionInterval = 1
	}
	if cfg.compactionWorkers < 1 {
		cfg.compactionWorkers = 1
	}
	if cfg.compactionThreshold >= 1.0 || cfg.compactionThreshold <= 0.01 {
		cfg.compactionThreshold = 0.10
	}
	if cfg.compactionAgeThreshold < 1 {
		cfg.compactionAgeThreshold = 1
	}
	return cfg
}

// OptList returns a slice with the opts given; useful if you want to possibly
// append more options to the list before using it with New(list...).
func OptList(opts ...func(*config)) []func(*config) {
	return opts
}

// OptLogCritical sets the log.Logger to use for critical messages. Defaults
// logging to os.Stderr.
func OptLogCritical(l *log.Logger) func(*config) {
	return func(cfg *config) {
		cfg.logCritical = l
	}
}

// OptLogError sets the log.Logger to use for error messages. Defaults logging
// to os.Stderr.
func OptLogError(l *log.Logger) func(*config) {
	return func(cfg *config) {
		cfg.logError = l
	}
}

// OptLogWarning sets the log.Logger to use for warning messages. Defaults
// logging to os.Stderr.
func OptLogWarning(l *log.Logger) func(*config) {
	return func(cfg *config) {
		cfg.logWarning = l
	}
}

// OptLogInfo sets the log.Logger to use for info messages. Defaults logging to
// os.Stdout.
func OptLogInfo(l *log.Logger) func(*config) {
	return func(cfg *config) {
		cfg.logInfo = l
	}
}

// OptLogDebug sets the log.Logger to use for debug messages. Defaults not
// logging debug messages.
func OptLogDebug(l *log.Logger) func(*config) {
	return func(cfg *config) {
		cfg.logDebug = l
	}
}

// OptRand sets the rand.Rand to use as a random data source. Defaults to a new
// randomizer based on the current time.
func OptRand(r *rand.Rand) func(*config) {
	return func(cfg *config) {
		cfg.rand = r
	}
}

// OptPath sets the path where values files will be written; tocvalues files
// will also be written here unless overridden with OptPathTOC. Defaults to env
// VALUESTORE_PATH or the current working directory.
func OptPath(dirpath string) func(*config) {
	return func(cfg *config) {
		cfg.path = dirpath
	}
}

// OptPathTOC sets the path where tocvalues files will be written. Defaults to
// env VALUESTORE_PATHTOC or the OptPath value.
func OptPathTOC(dirpath string) func(*config) {
	return func(cfg *config) {
		cfg.pathtoc = dirpath
	}
}

// OptValueLocMap allows overriding the default ValueLocMap, an interface used
// by ValueStore for tracking the mappings from keys to the locations of their
// values. Defaults to github.com/gholt/valuelocmap.New().
func OptValueLocMap(vlm valuelocmap.ValueLocMap) func(*config) {
	return func(cfg *config) {
		cfg.vlm = vlm
	}
}

// OptWorkers indicates how many goroutines may be used for various tasks
// (processing incoming writes and batching them to disk, background tasks,
// etc.). Defaults to env VALUESTORE_WORKERS or GOMAXPROCS.
func OptWorkers(count int) func(*config) {
	return func(cfg *config) {
		cfg.workers = count
	}
}

// OptTombstoneDiscardInterval indicates the minimum number of seconds betweeen
// the starts of discard passes (discarding expired tombstones [deletion
// markers]). If set to 60 seconds and the passes take 10 seconds to run, they
// will wait 50 seconds (with a small amount of randomization) between the stop
// of one run and the start of the next. This is really just meant to keep
// nearly empty structures from using a lot of resources doing nearly nothing.
// Normally, you'd want your discard passes to be running constantly so that
// they are as fast as possible and the load constant. The default of 60
// seconds is almost always fine. Defaults to env
// VALUESTORE_TOMBSTONEDISCARDINTERVAL or 60.
func OptTombstoneDiscardInterval(seconds int) func(*config) {
	return func(cfg *config) {
		cfg.tombstoneDiscardInterval = seconds
	}
}

// OptOutPullReplicationWorkers indicates how many goroutines may be used for
// an outgoing pull replication pass. Defaults to env
// VALUESTORE_OUTPULLREPLICATIONWORKERS or VALUESTORE_WORKERS.
func OptOutPullReplicationWorkers(workers int) func(*config) {
	return func(cfg *config) {
		cfg.outPullReplicationWorkers = workers
	}
}

// OptOutPullReplicationInterval indicates the minimum number of seconds
// between the starts of outgoing pull replication passes. If set to 60 seconds
// and the passes take 10 seconds to run, they will wait 50 seconds (with a
// small amount of randomization) between the stop of one run and the start of
// the next. This is really just meant to keep nearly empty structures from
// using a lot of resources doing nearly nothing. Normally, you'd want your
// outgoing pull replication passes to be running constantly so that
// replication is as fast as possible and the load constant. The default of 60
// seconds is almost always fine. Defaults to env
// VALUESTORE_OUTPULLREPLICATIONINTERVAL or 60.
func OptOutPullReplicationInterval(seconds int) func(*config) {
	return func(cfg *config) {
		cfg.outPullReplicationInterval = seconds
	}
}

// OptOutPushReplicationWorkers indicates how many goroutines may be used for
// an outgoing push replication pass. Defaults to env
// VALUESTORE_OUTPUSHREPLICATIONWORKERS or VALUESTORE_WORKERS.
func OptOutPushReplicationWorkers(workers int) func(*config) {
	return func(cfg *config) {
		cfg.outPushReplicationWorkers = workers
	}
}

// OptOutPushReplicationInterval indicates the minimum number of seconds
// between the starts of outgoing push replication passes. If set to 60 seconds
// and the passes take 10 seconds to run, they will wait 50 seconds (with a
// small amount of randomization) between the stop of one run and the start of
// the next. This is really just meant to keep nearly empty structures from
// using a lot of resources doing nearly nothing. Normally, you'd want your
// outgoing push replication passes to be running constantly so that
// replication is as fast as possible and the load constant. The default of 60
// seconds is almost always fine. Defaults to env
// VALUESTORE_OUTPUSHREPLICATIONINTERVAL or 60.
func OptOutPushReplicationInterval(seconds int) func(*config) {
	return func(cfg *config) {
		cfg.outPushReplicationInterval = seconds
	}
}

// OptMaxValueSize indicates the maximum number of bytes any given value may
// be. Defaults to env VALUESTORE_MAXVALUESIZE or 4,194,304.
func OptMaxValueSize(bytes int) func(*config) {
	return func(cfg *config) {
		cfg.maxValueSize = bytes
	}
}

// OptPageSize controls the size of each chunk of memory allocated. Defaults to
// env VALUESTORE_PAGESIZE or 4,194,304.
func OptPageSize(bytes int) func(*config) {
	return func(cfg *config) {
		cfg.pageSize = bytes
	}
}

// OptWritePagesPerWorker controls how many pages are created per worker for
// caching recently written values. Defaults to env
// VALUESTORE_WRITEPAGESPERWORKER or 3.
func OptWritePagesPerWorker(number int) func(*config) {
	return func(cfg *config) {
		cfg.writePagesPerWorker = number
	}
}

// OptTombstoneAge indicates how many seconds old a deletion marker may be
// before it is permanently removed. Defaults to env VALUESTORE_TOMBSTONEAGE or
// 14,400 (4 hours).
func OptTombstoneAge(seconds int) func(*config) {
	return func(cfg *config) {
		cfg.tombstoneAge = seconds
	}
}

// OptValuesFileSize indicates how large a values file can be before closing it
// and opening a new one. Defaults to env VALUESTORE_VALUESFILESIZE or
// 4,294,967,295.
func OptValuesFileSize(bytes int) func(*config) {
	return func(cfg *config) {
		cfg.valuesFileSize = bytes
	}
}

// OptValuesFileReaders indicates how many open file descriptors are allowed
// per values file for reading. Defaults to env VALUESTORE_VALUESFILEREADERS or
// the configured number of workers.
func OptValuesFileReaders(bytes int) func(*config) {
	return func(cfg *config) {
		cfg.valuesFileReaders = bytes
	}
}

// OptChecksumInterval indicates how many bytes are output to a file before a
// 4-byte checksum is also output. Defaults to env VALUESTORE_CHECKSUMINTERVAL
// or 65532.
func OptChecksumInterval(bytes int) func(*config) {
	return func(cfg *config) {
		cfg.checksumInterval = bytes
	}
}

// OptMsgRing sets the ring.MsgRing to use for determining the key ranges the
// ValueStore is responsible for as well as providing methods to send messages
// to other nodes.
func OptMsgRing(r ring.MsgRing) func(*config) {
	return func(cfg *config) {
		cfg.msgRing = r
	}
}

// OptReplicationIgnoreRecent indicates how many seconds old a value should be
// before it is included in replication processing. Defaults to env
// VALUESTORE_REPLICATIONIGNORERECENT or 60.
func OptReplicationIgnoreRecent(seconds int) func(*config) {
	return func(cfg *config) {
		cfg.replicationIgnoreRecent = seconds
	}
}
