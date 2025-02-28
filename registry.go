//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package oncache

import "sync"

var registry = make(map[string]Cache)
var registryLock sync.RWMutex

// Keep track of what caches are registered in order to find them by name during message
// handling. Panic if the name is already taken.
func registerCache(name string, cache Cache) {
	registryLock.Lock()
	defer registryLock.Unlock()

	if _, ok := registry[name]; ok {
		panic("cache " + name + " already exists")
	}
	registry[name] = cache
}

func getCacheByName(name string) Cache {
	registryLock.RLock()
	defer registryLock.RUnlock()

	return registry[name]
}
