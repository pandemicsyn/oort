package api

import "errors"

// got is at https://github.com/gholt/got
//go:generate got config.got valueconfig_GEN_.go TT=VALUE T=Value t=value
//go:generate got config.got groupconfig_GEN_.go TT=GROUP T=Group t=group
//go:generate got store.got valuestore_GEN_.go TT=VALUE T=Value t=value
//go:generate got store.got groupstore_GEN_.go TT=GROUP T=Group t=group
//go:generate got store_test.got valuestore_GEN_test.go TT=VALUE T=Value t=value
//go:generate got store_test.got groupstore_GEN_test.go TT=GROUP T=Group t=group
//go:generate got errorstore.got valueerrorstore_GEN_.go TT=VALUE T=Value t=value
//go:generate got errorstore.got grouperrorstore_GEN_.go TT=GROUP T=Group t=group

type s struct{}

func (*s) String() string {
	return "stats not available with this client at this time"
}

var noStats = &s{}

var noRingErr = errors.New("no ring")
