//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

// oncache provides a key-value cache plus a peer-to-peer broadcast system to deliver
// invalidation messages. Its purpose is an *easy-to-deploy* cache layer for a multi-node
// deployment. Invalidation delivery is O(n), so it is not intended for large scale.
package oncache

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

type Unixtime = int64
type Context context.Context

type signal struct {
	C      chan struct{}
	closer sync.Once
}

func (s *signal) wait() {
	<-s.C
}

func (s *signal) raise() {
	s.closer.Do(func() {
		close(s.C)
	})
}

func (s *signal) raised() bool {
	select {
	case <-s.C:
		return true
	default:
		return false
	}
}

func newSignal() signal {
	return signal{C: make(chan struct{})}
}

// An Oncache instance. Normally you only have one of these.
type Oncache struct {
	listenPort int
	activeWork *sync.WaitGroup

	caches         map[string]Cache
	cachesLock     sync.RWMutex
	shutdownSignal signal

	// Set during init.
	encryptionKey []byte
	live          bool

	// Networking

	peerLock             sync.RWMutex
	networkPeers         map[string]*networkPeer
	outgoingMessageQueue chan string
	incomingMessageQueue chan string
}

// Default port that Oncache listens on.
const DefaultPort = 7750

// Set the port to communicate on. This should be called before [Init]. Defaults to 7750.
func (oc *Oncache) SetPort(port int) {
	oc.listenPort = port
}

// Create a new Oncache instance. After creating an instance, you can configure it and
// then call [Init] to start it.
func New() *Oncache {
	return &Oncache{
		listenPort:     DefaultPort,
		activeWork:     &sync.WaitGroup{},
		caches:         make(map[string]Cache),
		shutdownSignal: signal{C: make(chan struct{})},

		networkPeers:         make(map[string]*networkPeer),
		outgoingMessageQueue: make(chan string, 1000),
		incomingMessageQueue: make(chan string, 1000),
	}
}

// Connect this instance to the list of hosts given. This function can be called multiple
// times.
//
// This function does not remove connected hosts, it only adds hosts that haven't been
// registered yet.
//
// Hosts are automatically removed when they are unreachable for an extended period of
// time. Hosts are automatically added when they connect to this instance remotely.
//
// Initial discovery of other peers is beyond the scope of this package.
func (oc *Oncache) Connect(hosts []string) {
	for _, host := range hosts {
		oc.registerPeer(host)
	}
}

// Invalidate a key prefix from the local caches (without propagation).
func (oc *Oncache) invalidateLocal(key string) {
	cacheName, key, found := strings.Cut(key, "/")
	if !found {
		// Invalid key.
		return
	}

	cache := oc.GetCache(cacheName)
	if cache == nil {
		// Cache not found.
		logWarn("Tried to invalidate key in cache that doesn't exist: " + cacheName)
		return
	}

	cache.Delete(key)
}

// Invalidate a key prefix. Do not include a trailing slash. This will evict all keys with
// the given prefix from the caches and then propagate an invalidation message to all
// peers.
func (oc *Oncache) Invalidate(key string) {
	oc.invalidateLocal(key)
	oc.DispatchMessage("1", "DEL "+key)
}

// This must be called during initialization. This sets the encryption key used between
// nodes. All nodes must use the same key to communicate. The key should be 16, 24, or 32
// crypto-random bytes.
func (oc *Oncache) Init(key []byte) error {
	if oc.live {
		return ErrAlreadyInitialized
	}

	if len(key) != 16 && len(key) != 24 && len(key) != 32 {
		return ErrInvalidKey
	}

	logInfo("Oncache initializing.")
	logInfo(fmt.Sprintf("keysize = %d", len(key)))
	oc.encryptionKey = key
	oc.live = true
	oc.shutdownSignal = newSignal()

	oc.activeWork.Add(3)
	go oc.messageSendProcess()
	go oc.listenerProcess()
	go oc.cleaningProcess()

	return nil
}

// Shut down the system. Called during application teardown. This will block until related
// goroutines quit.
func (oc *Oncache) Shutdown() {
	// When the shutdown signal is submitted, all processes should exit soon.
	oc.shutdownSignal.raise()

	// Wait for all processes to exit.
	oc.activeWork.Wait()
}

// Deferred call within processes to either
// (A) recover from panic and restart
// (B) exit normally and decrement the active work.
func (oc *Oncache) onProcessCompleted(name string, process func()) {
	if r := recover(); r != nil {
		logError(fmt.Sprintf("[%s] recovered from panic: %v; restarting in 60 seconds", name, r))
		logError(string(debug.Stack()))
		select {
		case <-time.After(time.Second * 60):
			// Delay 60 seconds for restart.
		case <-oc.shutdownSignal.C:
			logInfo("[%s] Shutdown signal received. Cancelling restart.")
			oc.activeWork.Done()
			return
		}
		process()
	} else {
		// Normal exit, process is done.
		oc.activeWork.Done()
	}
}

// Process that triggers automatic Clean calls.
func (oc *Oncache) cleaningProcess() {
	defer oc.onProcessCompleted(
		"garbageCollectionProcess",
		func() { oc.cleaningProcess() },
	)

	for {
		select {
		case <-time.After(time.Minute):
			// Wait 60 seconds between cleanup cycles.
			caches := oc.GetAllCaches()
			for _, cache := range caches {
				dc, ok := cache.(*dcache)
				if !ok {
					continue
				}

				if !dc.checkAndRunClean() {
					continue
				}

				select {
				case <-time.After(time.Millisecond * 200):
					// Wait 200ms between processing caches, so we don't fire off several
					// cleanups at once.
				case <-oc.shutdownSignal.C:
					// Quit if shutdown detected.
					return
				}
			}
		case <-oc.shutdownSignal.C:
			// Quit if shutdown detected.
			return
		}
	}
}

// Keep track of what caches are registered in order to find them by name during message
// handling. Does nothing and logs an error if the cache already exists.
func (oc *Oncache) registerCache(name string, cache Cache) {
	oc.cachesLock.Lock()
	defer oc.cachesLock.Unlock()

	if _, ok := oc.caches[name]; ok {
		logError("Tried to create cache \"" + name + "\" which already exists.")
		return
	}

	oc.caches[name] = cache
}

// Get a cache by name. Returns nil if the cache does not exist. Create caches with
// NewCache.
func (oc *Oncache) GetCache(name string) Cache {
	oc.cachesLock.RLock()
	defer oc.cachesLock.RUnlock()
	return oc.caches[name]
}

func (oc *Oncache) GetAllCaches() map[string]Cache {
	oc.cachesLock.RLock()
	defer oc.cachesLock.RUnlock()

	m := make(map[string]Cache)
	for k, v := range oc.caches {
		m[k] = v
	}

	return m
}
