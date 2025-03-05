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
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

type Unixtime = int64
type Context context.Context
type Hoststring = string

type MessageSubscriberId = int
type MessageHandler func(host Hoststring, channel string, message string)
type MessageSubscriber struct {
	id      MessageSubscriberId
	handler MessageHandler
}

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
	// The network port to listen to. Default 7750.
	listenPort int

	// Total work in progress. All processes increment this while they are working. The
	// shutdown process will wait for this to finish before returning.
	activeWork *sync.WaitGroup

	// Caches can only be added, not removed. Important for thread safety.
	caches     map[string]Cache
	cachesLock sync.RWMutex // For accessing or updating the caches map.

	// This is closed when the system is shutting down. All processes should check this
	// during sleep periods.
	shutdownSignal signal

	// Set during init
	// ---------------

	// AES encryption key to use. Must be 16, 24, or 32 bytes.
	encryptionKey []byte

	// Currently always "default". The encryption key name can be used to select different
	// keys from a set. We don't support multiple keys currently, but the encryption
	// protocol does.
	encryptionKeyName string

	// If the system is started (not sure if useful?).
	live bool

	// Networking
	// ----------

	// The local hostname.
	hostname string

	peerLock             sync.RWMutex
	networkPeers         map[Hoststring]*networkPeer
	outgoingMessageQueue chan string // Broadcast to all peers.
	incomingMessageQueue chan string // Received from any peer.

	subscriptionLock sync.Mutex

	// Grouped by channels. Empty entry "" matches all channels.
	subscriptions      map[string][]MessageSubscriber
	nextSubscriptionId int
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
	myHostWithoutPort, _, _ := strings.Cut(oc.hostname, ":")

	for _, host := range hosts {
		if host == oc.hostWithPort() {
			continue
		}
		hostWithoutPort, _, _ := strings.Cut(host, ":")
		if hostWithoutPort == myHostWithoutPort {
			logWarn("Remote hostname matches own (" + host + ").")
		}

		oc.registerPeer(host)
	}
}

// Invalidate a key prefix from the local caches (without propagation).
func (oc *Oncache) deleteLocal(fullkey string) {
	cacheName, key, found := strings.Cut(fullkey, "/")

	cache := oc.GetCache(cacheName)
	if cache == nil {
		// Cache not found.
		logWarn("Tried to delete key in cache that doesn't exist: " + cacheName)
		return
	}

	if found {
		cache.DeleteLocal(key)
	} else {
		cache.ResetLocal()
	}
}

// Delete a cache entry. The key format is <cachename>/<cachekey>. If you only include the
// cachename with no trailing slash, then the entire cache under that name will be reset.
func (oc *Oncache) Delete(key string) {
	oc.deleteLocal(key)
	oc.DispatchMessage("1", "DEL "+key)
}

// Returned when the key is not the expected format.
var ErrInvalidKey = errors.New("invalid key; must be 16, 24, or 32 bytes")

// If you call Init twice.
var ErrAlreadyInitialized = errors.New("already initialized")

func (oc *Oncache) hostWithPort() string {
	if strings.Contains(oc.hostname, ":") {
		return oc.hostname
	} else {
		return oc.hostname + ":" + fmt.Sprint(oc.listenPort)
	}
}

// This must be called during initialization. This sets the encryption key used between
// nodes. All nodes must use the same key to communicate. The key should be 16, 24, or 32
// crypto-random bytes.
//
// The hostname is where other nodes will connect to. It should be a hostname that is
// accessible from other nodes. If the public port differs from the local listen port, you
// can specify it with a `:port` suffix. Otherwise, the local listen port is attached to
// the hostname.
//
// Errors:
// - ErrAlreadyInitialized: If the system is already initialized.
// - ErrInvalidKey: If the key length is invalid.
func (oc *Oncache) Init(key []byte, hostname string) error {
	if oc.live {
		return ErrAlreadyInitialized
	}

	if len(key) != 16 && len(key) != 24 && len(key) != 32 {
		return ErrInvalidKey
	}

	logInfo("Oncache initializing.")
	logInfo(fmt.Sprintf("keysize = %d", len(key)))
	oc.encryptionKey = key
	oc.encryptionKeyName = "default"
	oc.hostname = hostname
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
// [Oncache.NewCache].
func (oc *Oncache) GetCache(name string) Cache {
	oc.cachesLock.RLock()
	defer oc.cachesLock.RUnlock()
	return oc.caches[name]
}

// Returns a copy of the caches map, so it can be iterated over safely.
func (oc *Oncache) GetAllCaches() map[string]Cache {
	oc.cachesLock.RLock()
	defer oc.cachesLock.RUnlock()

	m := make(map[string]Cache)
	for k, v := range oc.caches {
		m[k] = v
	}

	return m
}
