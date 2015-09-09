package main

import (
	"bufio"
	"log"
	"net"

	"github.com/pandemicsyn/ort/mapstore"
	"github.com/pandemicsyn/ort/ort"
	"github.com/pandemicsyn/ort/ortstore"
	"github.com/pandemicsyn/ort/rediscache"
)

func handle_conn(conn net.Conn, handler *rediscache.RESPhandler) {
	for {
		err := handler.Parse()
		if err != nil {
			conn.Close()
			return
		}
	}
}

func main() {
	ort := new(ort.Ort)
	err := ort.LoadConfig()
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
		log.Println(ort)
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

	readerChan := make(chan *bufio.Reader, 1024)
	for i := 0; i < cap(readerChan); i++ {
		readerChan <- nil
	}
	writerChan := make(chan *bufio.Writer, 1024)
	for i := 0; i < cap(writerChan); i++ {
		writerChan <- nil
	}
	handlerChan := make(chan *rediscache.RESPhandler, 1024)
	for i := 0; i < cap(handlerChan); i++ {
		handlerChan <- rediscache.NewRESPhandler(cache)
	}
	addr, err := net.ResolveTCPAddr("tcp", ort.ListenAddr)
	if err != nil {
		log.Println("Error getting IP: ", err)
		return
	}
	server, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Println("Error starting: ", err)
		return
	}
	log.Println("Listening on:", ort.ListenAddr)
	for {
		conn, _ := server.AcceptTCP()
		reader := <-readerChan
		if reader == nil {
			reader = bufio.NewReaderSize(conn, 65536)
		} else {
			reader.Reset(conn)
		}
		writer := <-writerChan
		if writer == nil {
			writer = bufio.NewWriterSize(conn, 65536)
		} else {
			writer.Reset(conn)
		}
		handler := <-handlerChan
		handler.Reset(reader, writer)
		go handle_conn(conn, handler)
		handlerChan <- handler
		writerChan <- writer
		readerChan <- reader
	}
}
