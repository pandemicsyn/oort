package ort

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/gholt/ring"
	"github.com/gholt/valuestore"
	"github.com/pandemicsyn/ort-syndicate/cmdctrl"
	"github.com/pandemicsyn/ort/rediscache"
	"github.com/pandemicsyn/ort/utils/srvconf"
)

var (
	CONFIG_CACHE_DIR  = "/var/cache"
	CONFIG_CACHE_FILE = "ortd-config.cache"
	STALE_CACHE_TIME  = 48 * time.Hour
)

// FExists true if a file or dir exists
func FExists(name string) bool {
	if _, err := os.Stat(name); os.IsNotExist(err) {
		return false
	}
	return true
}

type Server struct {
	sync.RWMutex
	StoreType        string
	ListenAddr       string
	RingFile         string    // The active ring file
	ring             ring.Ring // The active ring
	LocalID          uint64    // This nodes local ring id
	ValueStoreConfig valuestore.Config
	CmdCtrlConfig    cmdctrl.ConfigOpts
	cache            rediscache.Cache
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
func (o *Server) SetBackend(cache rediscache.Cache) {
	o.Lock()
	o.cache = cache
	o.Unlock()
}

func (o *Server) SetRing(r ring.Ring, ringFile string) {
	o.Lock()
	defer o.Unlock()
	o.ring = r
	o.RingFile = ringFile
	o.ring.SetLocalNode(o.LocalID)
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

func (o *Server) loadRingConfig() (err error) {
	o.Lock()
	defer o.Unlock()
	log.Println("Using ring version:", o.ring.Version())
	b := bytes.NewReader(o.ring.Conf())
	if b.Len() > 0 {
		_, err = toml.DecodeReader(b, o)
		if err != nil {
			return err
		}
	}
	// Now overlay per node config on top
	n := o.ring.LocalNode()
	if n == nil {
		panic("n is nil")
	}
	b = bytes.NewReader(o.ring.LocalNode().Conf())
	if b.Len() > 0 {
		_, err = toml.DecodeReader(b, o)
		if err != nil {
			return err
		}
	}
	log.Printf("Local Node config is: \n%s", o.ring.LocalNode().Conf())
	log.Printf("Ring config is: \n%s", o.ring.Conf())
	return nil
}

func (o *Server) LoadConfig() (err error) {
	envSkipSRV := os.Getenv("ORTD_SKIP_SRV")
	//First try and populate from cache.
	//Its fine if it doesn't exist or fails to load or is old
	var cached cacheConfig
	cacheLoaded := true
	_, err = toml.DecodeFile(filepath.Join(CONFIG_CACHE_DIR, CONFIG_CACHE_FILE), &cached)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Println("Cached config not found. Skipping.")
		} else {
			log.Println("Error loading cached config:", err)
		}
		cacheLoaded = false
	}
	if cacheLoaded {
		// TODO: IF the cache is "stale" should we really ignore it?
		if !time.Now().After(cached.CacheTime.Add(STALE_CACHE_TIME)) {
			log.Println("Using cached config")
			o.LocalID = cached.LocalID
			o.ValueStoreConfig = cached.ValueStoreConfig
			// a stale ring should be ok, since we're about to phone
			// home to verify the current version anyway.
			if cached.RingFile != "" {
				r, _, err := ring.RingOrBuilder(cached.RingFile)
				if err != nil {
					log.Println("Could not read cached ring file from disk", err)
				} else {
					o.RingFile = cached.RingFile
					o.ring = r
					o.ring.SetLocalNode(cached.LocalID)
					err = o.loadRingConfig()
					if err != nil {
						return err
					}
				}
			}
		} else {
			log.Println("Cache is considered stale, not using it.")
		}
	}

	// Check whether we're supposed to skip loading via srv method
	if strings.ToLower(envSkipSRV) != "true" {
		s := &srvconf.SRVLoader{
			Record:       genServiceID("syndicate", "tcp"),
			SyndicateURL: os.Getenv("ORT_SYNDICATE_OVERRIDE"),
		}
		if os.Getenv("ORT_SYNDICATE_OVERRIDE") != "" {
			log.Println("Overriding ort syndicate url with url from env!", os.Getenv("ORT_SYNDICATE_OVERRIDE"))
		}
		nc, err := s.Load()
		if err != nil {
			return err
		}
		o.ring, err = ring.LoadRing(bytes.NewReader(nc.Ring))
		if err != nil {
			return fmt.Errorf("Error while loading ring for config get via srv lookup: %s", err)
		}
		err = ring.PersistRingOrBuilder(o.ring, nil, fmt.Sprintf("/etc/ort/ortd/%d-ort.ring", o.ring.Version()))
		if err != nil {
			return err
		}
		o.LocalID = nc.Localid
		o.ring.SetLocalNode(o.LocalID)
		o.StoreType = "ortstore"
		o.RingFile = fmt.Sprintf("/etc/ort/ortd/%d-ort.ring", o.ring.Version())
		o.ListenAddr = "0.0.0.0:6379"
		err = o.loadRingConfig()
		if err != nil {
			return err
		}
	} else {
		// if you skip the srv load you have to provide all of the info in env vars!
		log.Println("Skipped SRV Config attempting to load from env")
		o.ListenAddr = os.Getenv("ORT_LISTEN_ADDRESS")
		s, err := strconv.ParseUint(os.Getenv("ORT_LOCALID"), 10, 64)
		if err != nil {
			return fmt.Errorf("Unable to load env specified local id")
		}
		o.LocalID = s
		o.RingFile = os.Getenv("ORT_RING_FILE")
		o.ring, _, err = ring.RingOrBuilder(o.RingFile)
		if err != nil {
			return fmt.Errorf("Unable to road env specified ring: %s", err)
		}
		o.ring.SetLocalNode(o.LocalID)
		err = o.loadRingConfig()
		if err != nil {
			return err
		}
	}
	// Allow overriding a few things via the env, that may be handy for debugging
	if os.Getenv("ORT_LISTEN_ADDRESS") != "" {
		o.ListenAddr = os.Getenv("ORT_LISTEN_ADDRESS")
	}
	return nil
}

type cacheConfig struct {
	LocalID          uint64             `toml:"LocalID"`
	ListenAddr       string             `toml:"ListenAddr"`
	RingFile         string             `toml:"RingFile"`
	CacheTime        time.Time          `toml:"CacheTime"`
	ValueStoreConfig valuestore.Config  `toml:"ValueStoreConfig"`
	CmdCtrlConfig    cmdctrl.ConfigOpts `toml:"CmdCtrlConfig"`
}

// CacheConfig caches a minimal config in
// /var/cache/ortd-config.cache
func (o *Server) CacheConfig() error {
	o.Lock()
	defer o.Unlock()
	c := cacheConfig{
		LocalID:          o.LocalID,
		ListenAddr:       o.ListenAddr,
		RingFile:         o.RingFile,
		CacheTime:        time.Now(),
		ValueStoreConfig: o.ValueStoreConfig,
		CmdCtrlConfig:    o.CmdCtrlConfig,
	}
	buf := new(bytes.Buffer)
	if err := toml.NewEncoder(buf).Encode(c); err != nil {
		return err
	}
	f, err := ioutil.TempFile(CONFIG_CACHE_DIR, CONFIG_CACHE_FILE+".tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	i, err := f.Write(buf.Bytes())
	if err != nil || i != len(buf.Bytes()) {
		f.Close()
		return fmt.Errorf("Error trying to write bytes to tmp file: %s", err)
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(CONFIG_CACHE_DIR, CONFIG_CACHE_FILE))
}

//TODO: need to remove the hack to add IAD3 identifier
func genServiceID(name, proto string) string {
	h, _ := os.Hostname()
	d := strings.SplitN(h, ".", 2)
	return fmt.Sprintf("_%s._%s.%s", name, proto, d[1])
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

func (o *Server) RingUpdate(newversion uint64, ringBytes []byte) (uint64, uint64) {
	log.Println("Got ring update:", newversion, ringBytes)
	return 0, 0
}

func (o *Server) Stats() []byte {
	return []byte("")
}

func (o *Server) Start() error {
	return nil
}

func (o *Server) Reload() error {
	return nil
}

func (o *Server) Restart() error {
	return nil
}

// shutdownFinished closes the ShutdownComplete channel
// 10 miliseconds after being invoked (to give a cmd ctrl client
// a chance to return.
func (o *Server) shutdownFinished() {
	time.Sleep(10 * time.Millisecond)
	close(o.ShutdownComplete)
}

// Stop the backend and shutdown all listeners.
// Closes the ShutdownComplete chan when finsihed.
func (o *Server) Stop() error {
	close(o.ch)
	o.waitGroup.Wait()
	o.cache.Stop()
	defer o.shutdownFinished()
	return nil
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
		handlerChan <- rediscache.NewRESPhandler(o.cache)
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
