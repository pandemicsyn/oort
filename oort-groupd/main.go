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
)

var (
	printVersionInfo = flag.Bool("version", false, "print version/build info")
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
	o, err := oort.New("group")
	if err != nil {
		log.Fatalln("Unable to obtain config:", err)
	}
	log.Println("Using groupstore backend")
	backend, err := oortstore.NewGroupStore(o)
	if err != nil {
		log.Fatalln("Unable to initialize GroupStore:", err)
	}
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
