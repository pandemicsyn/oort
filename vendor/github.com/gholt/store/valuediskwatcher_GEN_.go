package store

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ricochet2200/go-disk-usage/du"
)

type valueDiskWatcherState struct {
	interval               int
	freeDisableThreshold   uint64
	freeReenableThreshold  uint64
	usageDisableThreshold  float32
	usageReenableThreshold float32
	free                   uint64
	used                   uint64
	size                   uint64
	freetoc                uint64
	usedtoc                uint64
	sizetoc                uint64
	notifyChanLock         sync.Mutex
	notifyChan             chan *bgNotification
}

func (store *DefaultValueStore) diskWatcherConfig(cfg *ValueStoreConfig) {
	store.diskWatcherState.interval = 60
	store.diskWatcherState.freeDisableThreshold = cfg.FreeDisableThreshold
	store.diskWatcherState.freeReenableThreshold = cfg.FreeReenableThreshold
	store.diskWatcherState.usageDisableThreshold = cfg.UsageDisableThreshold
	store.diskWatcherState.usageReenableThreshold = cfg.UsageReenableThreshold
}

func (store *DefaultValueStore) EnableDiskWatcher() {
	store.diskWatcherState.notifyChanLock.Lock()
	if store.diskWatcherState.notifyChan == nil {
		store.diskWatcherState.notifyChan = make(chan *bgNotification, 1)
		go store.diskWatcherLauncher(store.diskWatcherState.notifyChan)
	}
	store.diskWatcherState.notifyChanLock.Unlock()
}

func (store *DefaultValueStore) DisableDiskWatcher() {
	store.diskWatcherState.notifyChanLock.Lock()
	if store.diskWatcherState.notifyChan != nil {
		c := make(chan struct{}, 1)
		store.diskWatcherState.notifyChan <- &bgNotification{
			action:   _BG_DISABLE,
			doneChan: c,
		}
		<-c
		store.diskWatcherState.notifyChan = nil
	}
	store.diskWatcherState.notifyChanLock.Unlock()
}

func (store *DefaultValueStore) diskWatcherLauncher(notifyChan chan *bgNotification) {
	interval := float64(store.diskWatcherState.interval) * float64(time.Second)
	store.randMutex.Lock()
	nextRun := time.Now().Add(time.Duration(interval + interval*store.rand.NormFloat64()*0.1))
	store.randMutex.Unlock()
	disabled := false
	running := true
	for running {
		var notification *bgNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-notifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-notifyChan:
			default:
			}
		}
		store.randMutex.Lock()
		nextRun = time.Now().Add(time.Duration(interval + interval*store.rand.NormFloat64()*0.1))
		store.randMutex.Unlock()
		if notification != nil {
			if notification.action == _BG_DISABLE {
				running = false
			} else {
				store.logCritical("diskWatcher: invalid action requested: %d", notification.action)
			}
			notification.doneChan <- struct{}{}
			continue
		}
		u := du.NewDiskUsage(store.path)
		utoc := u
		if store.pathtoc != store.path {
			utoc = du.NewDiskUsage(store.pathtoc)
		}
		free := u.Free()
		used := u.Used()
		size := u.Size()
		usage := u.Usage()
		freetoc := utoc.Free()
		usedtoc := utoc.Used()
		sizetoc := utoc.Size()
		usagetoc := utoc.Usage()
		atomic.StoreUint64(&store.diskWatcherState.free, free)
		atomic.StoreUint64(&store.diskWatcherState.used, used)
		atomic.StoreUint64(&store.diskWatcherState.size, size)
		atomic.StoreUint64(&store.diskWatcherState.freetoc, freetoc)
		atomic.StoreUint64(&store.diskWatcherState.usedtoc, usedtoc)
		atomic.StoreUint64(&store.diskWatcherState.sizetoc, sizetoc)
		if !disabled && store.diskWatcherState.freeDisableThreshold > 1 && (free <= store.diskWatcherState.freeDisableThreshold || freetoc <= store.diskWatcherState.freeDisableThreshold) {
			store.logCritical("diskWatcher: passed the free threshold for automatic disabling")
			store.enableWrites(false) // false indicates non-user call
			disabled = false
		}
		if !disabled && store.diskWatcherState.usageDisableThreshold > 0 && (usage >= store.diskWatcherState.usageDisableThreshold || usagetoc >= store.diskWatcherState.usageDisableThreshold) {
			store.logCritical("diskWatcher: passed the usage threshold for automatic disabling")
			store.enableWrites(false) // false indicates non-user call
			disabled = false
		}
		if disabled && store.diskWatcherState.freeReenableThreshold > 1 && free >= store.diskWatcherState.freeReenableThreshold && freetoc >= store.diskWatcherState.freeReenableThreshold {
			store.logCritical("diskWatcher: passed the free threshold for automatic re-enabling")
			store.enableWrites(false) // false indicates non-user call
			disabled = false
		}
		if disabled && store.diskWatcherState.usageReenableThreshold > 0 && usage <= store.diskWatcherState.usageReenableThreshold && usagetoc <= store.diskWatcherState.usageReenableThreshold {
			store.logCritical("diskWatcher: passed the usage threshold for automatic re-enabling")
			store.enableWrites(false) // false indicates non-user call
			disabled = false
		}
	}
}
