package oort

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/gholt/ring"
	"github.com/pandemicsyn/oort/utils/srvconf"
)

// FExists true if a file or dir exists
func FExists(name string) bool {
	if _, err := os.Stat(name); os.IsNotExist(err) {
		return false
	}
	return true
}

func (o *Server) loadRingConfig() (err error) {
	o.Lock()
	defer o.Unlock()
	log.Println("Using ring version:", o.ring.Version())
	b := bytes.NewReader(o.ring.Conf())
	if b.Len() > 0 {
		_, err = toml.DecodeReader(b, o)
		if err != nil {
			return err
		}
	}
	// Now overlay per node config on top
	n := o.ring.LocalNode()
	if n == nil {
		panic("n is nil")
	}
	b = bytes.NewReader(o.ring.LocalNode().Conf())
	if b.Len() > 0 {
		_, err = toml.DecodeReader(b, o)
		if err != nil {
			return err
		}
	}
	log.Printf("Local Node config is: \n%s", o.ring.LocalNode().Conf())
	log.Printf("Ring config is: \n%s", o.ring.Conf())
	return nil
}

func (o *Server) LoadConfig() (err error) {
	envSkipSRV := os.Getenv("OORTD_SKIP_SRV")
	// Check whether we're supposed to skip loading via srv method
	if strings.ToLower(envSkipSRV) != "true" {
		s := &srvconf.SRVLoader{
			SyndicateURL: os.Getenv("OORT_SYNDICATE_OVERRIDE"),
		}
		s.Record, err = genServiceID("syndicate", "tcp")
		if err != nil {
			if os.Getenv("OORT_SYNDICATE_OVERRIDE") == "" {
				log.Println(err)
			} else {
				log.Fatalln("No OORT_SYNDICATE_OVERRIDE provided and", err)
			}
		}
		if os.Getenv("OORT_SYNDICATE_OVERRIDE") != "" {
			log.Println("Overriding oort syndicate url with url from env!", os.Getenv("OORT_SYNDICATE_OVERRIDE"))
		}
		nc, err := s.Load()
		if err != nil {
			return err
		}
		o.ring, err = ring.LoadRing(bytes.NewReader(nc.Ring))
		if err != nil {
			return fmt.Errorf("Error while loading ring for config get via srv lookup: %s", err)
		}
		err = ring.PersistRingOrBuilder(o.ring, nil, fmt.Sprintf("/etc/oort/oortd/%d-oort.ring", o.ring.Version()))
		if err != nil {
			return err
		}
		o.LocalID = nc.Localid
		o.ring.SetLocalNode(o.LocalID)
		o.StoreType = "oortstore"
		o.RingFile = fmt.Sprintf("/etc/oort/oortd/%d-oort.ring", o.ring.Version())
		o.ListenAddr = "0.0.0.0:6379"
		err = o.loadRingConfig()
		if err != nil {
			return err
		}
	} else {
		// if you skip the srv load you have to provide all of the info in env vars!
		log.Println("Skipped SRV Config attempting to load from env")
		o.ListenAddr = os.Getenv("OORT_LISTEN_ADDRESS")
		s, err := strconv.ParseUint(os.Getenv("OORT_LOCALID"), 10, 64)
		if err != nil {
			return fmt.Errorf("Unable to load env specified local id")
		}
		o.LocalID = s
		o.RingFile = os.Getenv("OORT_RING_FILE")
		o.ring, _, err = ring.RingOrBuilder(o.RingFile)
		if err != nil {
			return fmt.Errorf("Unable to road env specified ring: %s", err)
		}
		o.ring.SetLocalNode(o.LocalID)
		err = o.loadRingConfig()
		if err != nil {
			return err
		}
	}
	// Allow overriding a few things via the env, that may be handy for debugging
	if os.Getenv("OORT_LISTEN_ADDRESS") != "" {
		o.ListenAddr = os.Getenv("OORT_LISTEN_ADDRESS")
	}
	return nil
}

//TODO: need to remove the hack to add IAD3 identifier
func genServiceID(name, proto string) (string, error) {
	h, _ := os.Hostname()
	d := strings.SplitN(h, ".", 2)
	if len(d) != 2 {
		return "", fmt.Errorf("Unable to determine FQDN, only got short name.")
	}
	return fmt.Sprintf("_%s._%s.%s", name, proto, d[1]), nil
}
