package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pandemicsyn/ort/mapstore"
	"github.com/pandemicsyn/ort/ort"
	"github.com/pandemicsyn/ort/ortstore"
	"github.com/pandemicsyn/ort/rediscache"
)

func main() {
	ort, err := ort.New()
	if err != nil {
		log.Println("Error loading config:", err)
		return
	}
	var cache rediscache.Cache
	switch ort.StoreType {
	case "map":
		log.Println("Using map cache")
		cache = mapstore.NewMapCache()
	case "ortstore":
		log.Println("Using ortstore (the gholt valuestore)")
		oc := ortstore.Config{
			Debug:   false,
			Profile: false,
		}
		cache = ortstore.New(ort, &oc)
	default:
		log.Printf("Got storetype: '%s' which isn't a valid backend\n", ort.StoreType)
		log.Println("Expected: map||ortstore")
		log.Println()
		return
	}

	go ort.Serve(cache)
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println(<-ch)
	// Stop the service gracefully.
	ort.Stop()
	cache.Stop()

}
