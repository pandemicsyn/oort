package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pandemicsyn/oort/oort"
	"github.com/pandemicsyn/oort/oortstore"
	"github.com/pandemicsyn/syndicate/utils/sysmetrics"
)

var (
	printVersionInfo = flag.Bool("version", false, "print version/build info")
	cwd              = flag.String("cwd", "/var/lib/oort-value", "the working directory use")
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
	o, err := oort.New("value", *cwd)
	if err != nil {
		log.Fatalln("Unable to obtain config:", err)
	}
	log.Println("Using valuestore backend")
	backend, err := oortstore.NewValueStore(o)
	if err != nil {
		log.Fatalln("Unable to initialize ValueStore:", err)
	}
	sysmetrics.StartupMetrics(backend.Config.MetricsAddr, backend.Config.MetricsCollectors)
	o.SetBackend(backend)
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
