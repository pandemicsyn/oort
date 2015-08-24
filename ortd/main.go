package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/pandemicsyn/ort/mapstore"
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

	conf, err := loadOrtConfig()
	if err != nil {
		fmt.Println("Error parsing config:", err)
		fmt.Println("Try -help for usage info")
		fmt.Println("The configuration is parsed in the following order: Ring Server via SRV, /etc/ort/ortd.toml, ENV")
		os.Exit(2)
	}

	var cache rediscache.Cache
	switch conf.StoreType {
	case "map":
		fmt.Println("Using map cache")
		cache = mapstore.NewMapCache()
	case "ortstore":
		log.Println(conf)
		fmt.Println("Using ortstore (the gholt valuestore)")
		cache = ortstore.New(conf.Ring, conf.RingFile, conf.localIDInt)
	default:
		fmt.Println("Nope:", conf.StoreType, "isn't a valid backend")
		fmt.Println("Try: map|ortstore")
		fmt.Println()
		os.Exit(2)
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
	addr, err := net.ResolveTCPAddr("tcp", conf.ListenAddr)
	if err != nil {
		fmt.Println("Error getting IP: ", err)
		return
	}
	server, err := net.ListenTCP("tcp", addr)
	if err != nil {
		fmt.Println("Error starting: ", err)
		return
	}
	fmt.Println("Listening on:", conf.ListenAddr)
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
