package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/pandemicsyn/photon/atomicstore"
	"github.com/pandemicsyn/photon/mapstore"
	"github.com/pandemicsyn/photon/rediscache"
	"github.com/pandemicsyn/photon/valuestore"
	"net"
	"os"
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

var storeType string
var listenAddr string

func init() {
	flag.StringVar(&listenAddr, "l", "127.0.0.1:6379", "host:port to listen on")
	flag.StringVar(&storeType, "s", "map", "which value store backend to use: [map|valuestore|atomic]")
	flag.Parse()
}

func main() {

	var cache rediscache.Cache

	switch storeType {
	case "map":
		fmt.Println("Using map cache")
		cache = mapstore.NewMapCache()
	case "valuestore":
		fmt.Println("Using valuestore")
		cache = valuestore.New()
	case "atomic":
		fmt.Println("Using atomic cache")
		cache = atomicstore.New(&atomicstore.VSConfig{})
	default:
		fmt.Println("Nope:", storeType, "isn't a valid backend")
		fmt.Println("Try: map|valuestore|atomic")
		fmt.Println()
		os.Exit(2)
	}

	fmt.Println("Starting...")
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
	addr, err := net.ResolveTCPAddr("tcp", listenAddr)
	if err != nil {
		fmt.Println("Error getting IP: ", err)
		return
	}
	server, err := net.ListenTCP("tcp", addr)
	if err != nil {
		fmt.Println("Error starting: ", err)
		return
	}
	fmt.Println("Listening on 6379")
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
