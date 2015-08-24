package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/gholt/ring"
	"github.com/pandemicsyn/ort/utils/srvconf"
)

// FExists true if a file or dir exists
func FExists(name string) bool {
	if _, err := os.Stat(name); os.IsNotExist(err) {
		return false
	}
	return true
}

type OrtConf struct {
	StoreType     string `default:"map"`
	ListenAddr    string `default:"localhost:6379"`
	RingFile      string `default:"/etc/ort/ort.ring"`
	Ring          ring.Ring
	localIDInt    uint64
	rawVstoreConf interface{}
}

func writeConfigToCache(c *OrtConf) error {
	f, err := os.Create("./.ortconf.cache")
	if err != nil {
		return err
	}
	defer f.Close()
	payload, err := json.Marshal(c)
	if err != nil {
		return err
	}
	s, err := f.Write(payload)
	if s != len(payload) {
		return fmt.Errorf("Config written to cache probably incorrect")
	}
	f.Sync()
	return nil
}

//TODO: need to remove the hack to add IAD3 identifier
func genServiceID(name, proto string) string {
	h, _ := os.Hostname()
	d := strings.SplitN(h, ".", 2)
	if !strings.HasPrefix(d[1], "iad3") {
		log.Println("Using pre production dev hack to add DC!!! Remove me")
		return fmt.Sprintf("_%s._%s.iad3.%s", name, proto, d[1])
	}
	return fmt.Sprintf("_%s._%s.%s", name, proto, d[1])
}

func loadOrtConfig() (*OrtConf, error) {
	oc := new(OrtConf)
	s := &srvconf.SRVLoader{Record: genServiceID("ring", "tcp")}
	nc, err := s.Load()
	if err != nil {
		return oc, err
	}

	oc.Ring, err = ring.LoadRing(bytes.NewReader(nc.Ring))
	if err != nil {
		return oc, fmt.Errorf("Error while loading ring for config get via srv lookup: %s", err)
	}

	err = ring.PersistRingOrBuilder(oc.Ring, nil, fmt.Sprintf("/etc/ort/ort.ring-%d", oc.Ring.Version()))
	if err != nil {
		return oc, err
	}
	oc.RingFile = fmt.Sprintf("/etc/ort/ort.ring-%d", oc.Ring.Version())
	oc.StoreType = "ortstore"
	oc.localIDInt = nc.Localid
	oc.ListenAddr = "127.0.0.1:6379"
	err = writeConfigToCache(oc)
	return oc, err
}
