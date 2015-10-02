package ort

import (
	"bufio"
	"log"
	"net"
	"sync"
	"time"

	"github.com/gholt/ring"
	"github.com/gholt/valuestore"
	"github.com/pandemicsyn/ort-syndicate/cmdctrl"
	"github.com/pandemicsyn/ort/rediscache"
)

type Server struct {
	sync.RWMutex
	StoreType        string
	ListenAddr       string
	RingFile         string    // The active ring file
	ring             ring.Ring // The active ring
	LocalID          uint64    // This nodes local ring id
	ValueStoreConfig valuestore.Config
	CmdCtrlConfig    cmdctrl.ConfigOpts
	backend          rediscache.Cache
	ch               chan bool
	ShutdownComplete chan bool
	waitGroup        *sync.WaitGroup
}

func New() (*Server, error) {
	o := &Server{
		ch:               make(chan bool),
		ShutdownComplete: make(chan bool),
		waitGroup:        &sync.WaitGroup{},
	}
	o.waitGroup.Add(1)
	err := o.LoadConfig()
	return o, err
}

//SetBackend sets the current backend
func (o *Server) SetBackend(backend rediscache.Cache) {
	o.Lock()
	o.backend = backend
	o.Unlock()
}

func (o *Server) SetRing(r ring.Ring, ringFile string) {
	o.Lock()
	defer o.Unlock()
	log.Println("FH - in SetRing")
	o.ring = r
	o.RingFile = ringFile
	log.Println("FH - about to set local node")
	o.ring.SetLocalNode(o.LocalID)
	log.Println("FH - about to update backend")
	o.backend.UpdateRing(o.ring)
}

// Ring returns an instance of the current Ring
func (o *Server) Ring() ring.Ring {
	o.RLock()
	defer o.RUnlock()
	return o.ring
}

// GetLocalID returns the current local id
func (o *Server) GetLocalID() uint64 {
	o.RLock()
	defer o.RUnlock()
	return o.LocalID
}

func (o *Server) handle_conn(conn net.Conn, handler *rediscache.RESPhandler) {
	defer conn.Close()
	defer o.waitGroup.Done()
	for {
		err := handler.Parse()
		if err != nil {
			return
		}
	}
}

// Serve starts the command and control instance, as well as the backend
func (o *Server) Serve() {
	defer o.waitGroup.Done()
	if o.CmdCtrlConfig.Enabled {
		cc := cmdctrl.NewCCServer(o, &o.CmdCtrlConfig)
		go cc.Serve()
	} else {
		log.Println("Command and Control functionality disabled via config")
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
		handlerChan <- rediscache.NewRESPhandler(o.backend)
	}
	addr, err := net.ResolveTCPAddr("tcp", o.ListenAddr)
	if err != nil {
		log.Println("Error getting IP: ", err)
		return
	}
	server, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Println("Error starting: ", err)
		return
	}
	log.Println("Listening on:", o.ListenAddr)
	for {
		select {
		case <-o.ch:
			log.Println("Shutting down")
			server.Close()
			return
		default:
		}
		server.SetDeadline(time.Now().Add(1e9))
		conn, err := server.AcceptTCP()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}
			log.Println(err)
		}
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
		o.waitGroup.Add(1)
		go o.handle_conn(conn, handler)
		handlerChan <- handler
		writerChan <- writer
		readerChan <- reader
	}
}
