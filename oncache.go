//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

// oncache provides a key-value cache plus a peer-to-peer broadcast system to deliver
// invalidation messages. Its purpose is an *easy-to-deploy* cache layer for a multi-node
// deployment. Invalidation delivery is O(n), so it is not intended for large scale.
package oncache

import (
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

type Unixtime = int64

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

var networkPort = 7750
var myHosts []string
var encryptionKey []byte
var initCalled bool
var shutdownSignal signal
var workWaitGroup sync.WaitGroup

// Set the port to communicate on. This should be called before init. The default port is
// 7750.
func SetPort(port int) {
	networkPort = port
}

// Connect this node to the list of hosts given. This function can be called multiple
// times during the application life.
//
// This function does not remove connected hosts, it only adds hosts that haven't been
// registered yet.
//
// Hosts are automatically removed when they are unreachable for an extended period of
// time. Hosts are automatically added when they connect to this node.
//
// Initial discovery of other peers is beyond the scope of this package.
func Connect(hosts []string) {
	myHosts = hosts
}

func invalidateLocal(key string) {
	cacheName, key, found := strings.Cut(key, "/")
	if !found {
		// Invalid key.
		return
	}

	cache := getCacheByName(cacheName)
	if cache == nil {
		// Cache not found.
		return
	}

	cache.Delete(key)
}

// Invalidate a key prefix.
func Invalidate(key string) {
	invalidateLocal(key)
	DispatchMessage("1", "DEL "+key)
}

// This must be called during initialization. This sets the encryption key used between
// nodes. All nodes must use the same key to communicate. The key should be 16, 24, or 32
// crypto-random bytes. This can be called again later to change the key.
func Init(key []byte) error {
	if len(key) != 16 && len(key) != 24 && len(key) != 32 {
		return ErrInvalidKey
	}

	logInfo("Oncache initializing.")
	logInfo(fmt.Sprintf("keysize = %d", len(key)))
	encryptionKey = key
	initCalled = true
	shutdownSignal = signal{C: make(chan struct{})}
	workWaitGroup.Add(2)

	go messageSendProcess(&workWaitGroup)
	go listenerProcess(&workWaitGroup)
	return nil
}

// Shut down the system. Called during application teardown. This will block until related
// goroutines quit.
func Shutdown() {
	shutdownSignal.raise()
	workWaitGroup.Wait()

}

func catchProcessPanic(name string, wg *sync.WaitGroup, process func()) {
	if r := recover(); r != nil {
		logError(fmt.Sprintf("[%s] recovered from panic: %v; restarting in 60 seconds", name, r))
		logError(string(debug.Stack()))
		select {
		case <-time.After(time.Second * 60):
			process()
		case <-shutdownSignal.C:
			logInfo("[%s] Shutdown signal received. Cancelling restart.")
			wg.Done()
			return
		}
		process()
	} else {
		wg.Done()
	}
}
