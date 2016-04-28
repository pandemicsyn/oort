package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/pandemicsyn/oort/oort"
	"github.com/pandemicsyn/oort/oortstore"
	"github.com/pandemicsyn/syndicate/utils/sysmetrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	printVersionInfo  = flag.Bool("version", false, "print version/build info")
	cwd               = flag.String("cwd", "/var/lib/oort-value", "the working directory use")
	enabledCollectors = flag.String("collectors", sysmetrics.FilterAvailableCollectors(sysmetrics.DefaultCollectors), "Comma-separated list of collectors to use.")
)
var oortVersion string
var ringVersion string
var valuestoreVersion string
var cmdctrlVersion string
var goVersion string
var buildDate string

func setupMetrics() {
	collectors, err := sysmetrics.LoadCollectors(*enabledCollectors)
	if err != nil {
		log.Fatalf("Couldn't load collectors: %s", err)
	}
	nodeCollector := sysmetrics.New(collectors)
	prometheus.MustRegister(nodeCollector)
	http.Handle("/metrics", prometheus.Handler())
	go http.ListenAndServe(":9100", nil)
}

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
	o, err := oort.New("value", *cwd)
	if err != nil {
		log.Fatalln("Unable to obtain config:", err)
	}
	log.Println("Using valuestore backend")
	backend, err := oortstore.NewValueStore(o)
	if err != nil {
		log.Fatalln("Unable to initialize ValueStore:", err)
	}
	o.SetBackend(backend)
	o.Serve()

	setupMetrics()

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
