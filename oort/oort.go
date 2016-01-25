package oort

import (
	"log"
	"sync"

	"github.com/gholt/ring"
	"github.com/pandemicsyn/cmdctrl"
)

type OortService interface {
	Stats() []byte
	//Start is called before ListenAndServe to startup any needed stuff
	Start()
	//Stop is called before StopListenAndServe
	Stop()
	//The method we'll invoke when we receive a new ring
	UpdateRing(ring.Ring)
	// ListenAndServe is assumed to bind to an address and just handle/pass off requests, Start is called BEFORE this to make sure
	// any need backend services/chan's are up and running before we start accepting requests.
	ListenAndServe()
	// StopListenAndServe is assumed to only stop the OortService's network listener. It shouldn't return as soon as
	// the service is no longer listening on the interface
	StopListenAndServe()
	// Wait() should block until all active requests are serviced (or return immediately if not implemented).
	Wait()
}

type Server struct {
	sync.RWMutex
	serviceName string
	ringFile    string
	ring        ring.Ring
	localID     uint64
	backend     OortService //the backend service
	// TODO: should probably share ch with backend so a stop on one stops both.
	ch                chan bool //os signal chan,
	ShutdownComplete  chan bool
	waitGroup         *sync.WaitGroup
	cmdCtrlLock       sync.RWMutex
	CmdCtrlConfig     cmdctrl.ConfigOpts
	cmdCtrlLoopActive bool
	stopped           bool
}

func New(serviceName string) (*Server, error) {
	o := &Server{
		serviceName:      serviceName,
		ch:               make(chan bool),
		ShutdownComplete: make(chan bool),
		waitGroup:        &sync.WaitGroup{},
		stopped:          false,
	}
	err := o.ObtainConfig()
	if err != nil {
		return o, err
	}
	return o, err
}

//SetBackend sets the current backend
func (o *Server) SetBackend(backend OortService) {
	o.Lock()
	o.backend = backend
	o.Unlock()
}

func (o *Server) SetRing(r ring.Ring, ringFile string) {
	o.Lock()
	o.ring = r
	o.ringFile = ringFile
	o.ring.SetLocalNode(o.localID)
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
	return o.localID
}

func (o *Server) CmdCtrlLoopActive() bool {
	o.RLock()
	defer o.RUnlock()
	return o.cmdCtrlLoopActive
}

func (o *Server) runCmdCtrlLoop() {
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
}

func (o *Server) Serve() {
	defer o.waitGroup.Done()
	o.waitGroup.Add(1)
	if o.CmdCtrlConfig.Enabled {
		o.runCmdCtrlLoop()
	} else {
		log.Println("Command and Control functionality disabled via config")
	}
	go o.backend.ListenAndServe()
}
