package oort

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"time"

	"github.com/gholt/ring"
)

func writeBytes(filename string, b *[]byte) error {
	dir, name := path.Split(filename)
	if dir == "" {
		dir = "."
	}
	f, err := ioutil.TempFile(dir, name+".")
	if err != nil {
		return err
	}
	tmp := f.Name()
	s, err := f.Write(*b)
	if err != nil {
		f.Close()
		return err
	}
	if s != len(*b) {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, filename)
}

func (o *Server) RingUpdate(newversion int64, ringBytes []byte) int64 {
	o.cmdCtrlLock.Lock()
	defer o.cmdCtrlLock.Unlock()
	log.Println("Got ring update notification. Trying to update to version:", newversion)
	newring, err := ring.LoadRing(bytes.NewReader(ringBytes))
	if err != nil {
		log.Println("Error loading ring during update:", err)
		return o.Ring().Version()
	}
	if newring.Version() != newversion {
		log.Println("Provided ring version != version in ring")
		return o.Ring().Version()
	}
	fname := fmt.Sprintf("/etc/oort/oortd/%d-oort.ring", newring.Version())
	writeBytes(fname, &ringBytes)
	o.SetRing(newring, fname)
	return o.Ring().Version()
}

func (o *Server) Stats() []byte {
	return o.backend.Stats()
}

func (o *Server) Start() error {
	o.cmdCtrlLock.Lock()
	defer o.cmdCtrlLock.Unlock()
	if !o.stopped {
		return fmt.Errorf("Service already running")
	}
	o.ch = make(chan bool)
	o.backend.Start()
	go o.serve()
	return nil
}

func (o *Server) Reload() error {
	return nil
}

func (o *Server) Restart() error {
	o.cmdCtrlLock.Lock()
	defer o.cmdCtrlLock.Unlock()
	if o.stopped {
		return fmt.Errorf("Service not running")
	}
	close(o.ch)
	o.waitGroup.Wait()
	o.backend.Stop()
	o.ch = make(chan bool)
	o.backend.Start()
	go o.serve()
	return nil
}

func (o *Server) HealthCheck() (bool, string) {
	return true, "pong"
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
// Does NOT exist the process.
func (o *Server) Stop() error {
	o.cmdCtrlLock.Lock()
	defer o.cmdCtrlLock.Unlock()
	if o.stopped {
		return fmt.Errorf("Service already stopped")
	}
	close(o.ch)
	o.waitGroup.Wait()
	o.backend.Stop()
	return nil
}

// Exit the backend and shutdown all listeners.
// Closes the ShutdownComplete chan when finsihed.
func (o *Server) Exit() error {
	o.cmdCtrlLock.Lock()
	defer o.cmdCtrlLock.Unlock()
	if o.stopped {
		o.backend.Stop()
		defer o.shutdownFinished()
		return nil
	}
	close(o.ch)
	o.waitGroup.Wait()
	o.backend.Stop()
	defer o.shutdownFinished()
	return nil
}
