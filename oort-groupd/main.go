package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/pandemicsyn/oort/mapstore"
	"github.com/pandemicsyn/oort/oort"
	"github.com/pandemicsyn/oort/oortstore"
)

var (
	printVersionInfo = flag.Bool("version", false, "print version/build info")
	noTLS            = flag.Bool("notls", false, "whether to disable tls")
	certFile         = flag.String("certfile", "/etc/oort/server.crt", "path to ssl crt")
	keyFile          = flag.String("keyfile", "/etc/oort/server.key", "path to ssl key")
	maxClients       = flag.Int("max-clients", 8192, "")
	storeType        = flag.String("storetype", "valuestore", "storetype to boot")
)
var oortVersion string
var ringVersion string
var valuestoreVersion string
var cmdctrlVersion string
var goVersion string
var buildDate string

func main() {
	flag.Parse()
	if *printVersionInfo {
		fmt.Println("oort version:", oortVersion)
		fmt.Println("ring version:", ringVersion)
		fmt.Println("cmdctrl version:", cmdctrlVersion)
		fmt.Println("valuestore version:", valuestoreVersion)
		fmt.Println("build date:", buildDate)
		fmt.Println("go version:", goVersion)
		return
	}
	envStoreType := os.Getenv("OORT_STORETYPE")
	if envStoreType != "" {
		*storeType = envStoreType
	}
	envTLS := os.Getenv("OORT_NO_TLS")
	if envTLS == "true" {
		*noTLS = true
	}
	envMX := os.Getenv("OORT_MAX_CLIENTS")
	if envMX != "" {
		v, err := strconv.Atoi(envMX)
		if err != nil {
			log.Println("Did not sent max clients from env:", err)
		} else {
			*maxClients = v
		}
	}
	o, err := oort.New()
	if err != nil {
		log.Fatalln("Unable to obtain config:", err)
	}
	switch *storeType {
	case "map":
		log.Println("Using map cache")
		o.SetBackend(mapstore.NewMapCache())
	case "valuestore":
		log.Println("Using valuestore backend")
		backend, err := oortstore.NewValueStore(o)
		if err != nil {
			log.Fatalln("Unable to initialize ValueStore:", err)
		}
		o.SetBackend(backend)
	default:
		log.Printf("Got storetype: '%s' which isn't a valid backend\n", *storeType)
		log.Println("Expected: map||valuestore")
		log.Println()
		return
	}
	o.Serve()
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-ch:
			o.Exit()
			<-o.ShutdownComplete
			return
		case <-o.ShutdownComplete:
			return
		}
	}
}
