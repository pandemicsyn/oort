package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pandemicsyn/ort/mapstore"
	"github.com/pandemicsyn/ort/ort"
	"github.com/pandemicsyn/ort/ortstore"
)

func main() {
	ort, err := ort.New()
	if err != nil {
		log.Println("Error loading config:", err)
		return
	}
	switch ort.StoreType {
	case "map":
		log.Println("Using map cache")
		ort.SetBackend(mapstore.NewMapCache())
	case "ortstore":
		log.Println("Using ortstore (the gholt valuestore)")
		oc := ortstore.Config{
			Debug:   false,
			Profile: false,
		}
		ort.SetBackend(ortstore.New(ort, &oc))
	default:
		log.Printf("Got storetype: '%s' which isn't a valid backend\n", ort.StoreType)
		log.Println("Expected: map||ortstore")
		log.Println()
		return
	}
	go ort.Serve()
	log.Println(ort.CmdCtrlConfig)
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-ch:
			ort.Stop()
			<-ort.ShutdownComplete
			return
		case <-ort.ShutdownComplete:
			return
		}
	}
}
