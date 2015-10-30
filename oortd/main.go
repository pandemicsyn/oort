package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pandemicsyn/oort/mapstore"
	"github.com/pandemicsyn/oort/oort"
	"github.com/pandemicsyn/oort/oortstore"
)

func main() {
	o, err := oort.New()
	if err != nil {
		log.Println("Error loading config:", err)
		return
	}
	switch o.StoreType {
	case "map":
		log.Println("Using map cache")
		o.SetBackend(mapstore.NewMapCache())
	case "oortstore":
		log.Println("Using oortstore (the gholt valuestore)")
		oc := oortstore.Config{
			Debug:   false,
			Profile: false,
		}
		o.SetBackend(oortstore.New(o, &oc))
	default:
		log.Printf("Got storetype: '%s' which isn't a valid backend\n", o.StoreType)
		log.Println("Expected: map||oortstore")
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
