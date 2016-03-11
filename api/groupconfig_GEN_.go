package api

import "google.golang.org/grpc"

// ReplGroupStoreConfig defines the settings when calling NewGroupStore.
type ReplGroupStoreConfig struct {
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
	// GRPCOpts are any additional options you'd like to pass to GRPC.
	GRPCOpts []grpc.DialOption
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
