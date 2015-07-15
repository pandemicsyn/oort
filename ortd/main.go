package main

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"github.com/pandemicsyn/ort/mapstore"
	"github.com/pandemicsyn/ort/ortstore"
	"github.com/pandemicsyn/ort/rediscache"
	"github.com/spf13/viper"
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

func main() {

	viper.SetDefault("listenAddr", "127.0.0.1:6379")
	viper.SetDefault("ringFile", "/etc/ort/ort.ring")
	viper.SetDefault("storeType", "map")
	viper.SetDefault("localID", 0)

	viper.SetEnvPrefix("ort")

	viper.BindEnv("listenAddr")
	viper.BindEnv("ringFile")
	viper.BindEnv("storeType")
	viper.BindEnv("localID")

	viper.SetConfigName("ortd")        // name of config file (without extension)
	viper.AddConfigPath("/etc/ort/")   // path to look for the config file in
	viper.AddConfigPath("$HOME/.ortd") // call multiple times to add many search paths
	viper.ReadInConfig()               // Find and read the config file

	storeType := viper.GetString("storeType")
	listenAddr := viper.GetString("listenAddr")
	ringLocalID := viper.GetInt("localID")

	var cache rediscache.Cache
	switch storeType {
	case "map":
		fmt.Println("Using map cache")
		cache = mapstore.NewMapCache()
	case "ortstore":
		fmt.Println("Using ortstore (the gholt valuestore)")
		cache = ortstore.New(viper.GetString("ringFile"), ringLocalID)
	default:
		fmt.Println("Nope:", storeType, "isn't a valid backend")
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
	fmt.Println("Listening on:", listenAddr)
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
