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
	"strings"
)

var networkPort = 7750
var myHosts []string
var encryptionKey []byte
var initCalled bool
var shutdownSignal chan struct{}
var shutdownComplete chan struct{}

// Set the port to communicate on. This can be called at any time. The default port is
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

// Invalidate a key prefix.
func Invalidate(key string) {
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

	DispatchMessage("DELETE " + key)
}

// This must be called during initialization. This sets the encryption key used between
// nodes. All nodes must use the same key to communicate. The key should be 16, 24, or 32
// crypto-random bytes.
func Init(key []byte) error {
	if len(key) != 16 && len(key) != 24 && len(key) != 32 {
		return ErrInvalidKey
	}

	logInfo("Oncache initializing.")
	logInfo(fmt.Sprintf("keysize = %d", len(key)))
	encryptionKey = key
	initCalled = true
	shutdownSignal = make(chan struct{})
	shutdownComplete = make(chan struct{})

	go messageLoop()
	return nil
}

func Shutdown() {
	select {
	case <-shutdownSignal:
	default:
		close(shutdownSignal)
	}

	<-shutdownComplete
}

func messageLoop() {
	defer close(shutdownComplete)

	for {
		select {
		case message := <-messageQueue:
			handleMessage(message)
		case <-shutdownSignal:
			return
		}
	}
}
