package oort

import (
	"bufio"
	"log"
	"net"
	"sync"
	"time"

	"github.com/gholt/ring"
	"github.com/gholt/valuestore"
	"github.com/pandemicsyn/oort/rediscache"
	"github.com/pandemicsyn/syndicate/cmdctrl"
)

type Server struct {
	sync.RWMutex
	StoreType         string
	ListenAddr        string
	RingFile          string    // The active ring file
	ring              ring.Ring // The active ring
	LocalID           uint64    // This nodes local ring id
	ValueStoreConfig  valuestore.Config
	CmdCtrlConfig     cmdctrl.ConfigOpts
	cmdCtrlLoopActive bool
	backend           rediscache.Cache
	ch                chan bool
	ShutdownComplete  chan bool
	waitGroup         *sync.WaitGroup
	cmdCtrlLock       sync.RWMutex
	stopped           bool
}

func New() (*Server, error) {
	o := &Server{
		ch:               make(chan bool),
		ShutdownComplete: make(chan bool),
		waitGroup:        &sync.WaitGroup{},
		stopped:          false,
	}
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
	o.ring = r
	o.RingFile = ringFile
	o.ring.SetLocalNode(o.LocalID)
	o.backend.UpdateRing(o.ring)
	log.Println("Ring version is now:", o.ring.Version())
	o.Unlock()
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

func (o *Server) CmdCtrlLoopActive() bool {
	o.RLock()
	defer o.RUnlock()
	return o.cmdCtrlLoopActive
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

// serve sets up all the channels/listeners. its also invoked by
// cmdctrl start/restart to turn services back on.
func (o *Server) serve() {
	defer o.waitGroup.Done()
	o.waitGroup.Add(1)
	if o.CmdCtrlConfig.Enabled {
		if !o.cmdCtrlLoopActive {
			go func(o *Server) {
				firstAttempt := true
				for {
					o.cmdCtrlLoopActive = true
					cc := cmdctrl.NewCCServer(o, &o.CmdCtrlConfig)
					err := cc.Serve()
					if err != nil && firstAttempt {
						//since this is our first attempt to bind/serve and we blew up
						//we're probably missing something import and wont be able to
						//recover.
						log.Fatalln("Error on first attempt to launch CmdCtrl Serve")
					} else if err != nil && !firstAttempt {
						log.Println("CmdCtrl Serve encountered error:", err)
					} else {
						log.Println("CmdCtrl Serve exited without error, quiting")
						break
					}
					firstAttempt = false
				}
			}(o)
		}
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

// Serve starts the command and control instance, as well as the backend
func (o *Server) Serve() {
	go o.serve()
}
