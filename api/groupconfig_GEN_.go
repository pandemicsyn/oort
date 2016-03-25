package api

import "google.golang.org/grpc"

// ReplGroupStoreConfig defines the settings when calling NewGroupStore.
type ReplGroupStoreConfig struct {
	// LogError sets the func to use for error messages. Defaults to stderr.
	LogError func(fmt string, args ...interface{})
	// LogDebug sets the func to use for debug messages. Defaults to not
	// logging debug messages.
	LogDebug func(fmt string, args ...interface{})
	// AddressIndex indicates which of the ring node addresses to use when
	// connecting to a node (see github.com/gholt/ring/Node.Address).
	AddressIndex int
	// ValueCap defines the maximum value size supported by the set of stores.
	// This defaults to 0xffffffff, or math.MaxUint32. In order to discover the
	// true value cap, all stores would have to be queried and then the lowest
	// cap used. However, that's probably not really necessary and configuring
	// a set value cap here is probably fine.
	ValueCap uint32
	// ConcurrentRequestsPerStore defines the concurrent requests per
	// underlying connected store. Default: 10
	ConcurrentRequestsPerStore int
	// FailedConnectRetryDelay defines how many seconds must pass before
	// retrying a failed connection. Default: 15 seconds
	FailedConnectRetryDelay int
	// GRPCOpts are any additional options you'd like to pass to GRPC when
	// connecting to stores.
	GRPCOpts []grpc.DialOption
	// RingServer is the network address to use to connect to a ring server. An
	// empty string will use the default DNS method of determining the ring
	// server location.
	RingServer string
	// RingServerGRPCOpts are any additional options you'd like to pass to GRPC
	// when connecting to the ring server.
	RingServerGRPCOpts []grpc.DialOption
	// RingCachePath is the full location file name where you'd like persist
	// last received ring data, such as "/var/lib/myprog/ring/valuestore.ring".
	// An empty string will disable caching. The cacher will need permission to
	// create a new file with the path given plus a temporary suffix, and will
	// then move that temporary file into place using the exact path given.
	RingCachePath string
}

func resolveReplGroupStoreConfig(c *ReplGroupStoreConfig) *ReplGroupStoreConfig {
	cfg := &ReplGroupStoreConfig{}
	if c != nil {
		*cfg = *c
	}
	if cfg.ValueCap == 0 {
		cfg.ValueCap = 0xffffffff
	}
	if cfg.ConcurrentRequestsPerStore == 0 {
		cfg.ConcurrentRequestsPerStore = 10
	}
	if cfg.ConcurrentRequestsPerStore < 1 {
		cfg.ConcurrentRequestsPerStore = 1
	}
	if cfg.FailedConnectRetryDelay == 0 {
		cfg.FailedConnectRetryDelay = 15
	}
	if cfg.FailedConnectRetryDelay < 1 {
		cfg.FailedConnectRetryDelay = 1
	}
	return cfg
}
