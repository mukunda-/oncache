//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

/*
This package provides a simple map-based cache with no eviction strategy.
*/
package mapcache

import "time"

var Now = func() int64 {
	return time.Now().Unix()
}

type cacheEntry struct {
	Value      any
	Expiration int32
}

type MapCache struct {
	// Map of all keys to data records.
	keys map[string]cacheEntry

	// Offset for expiration times.
	epoch int64
}

func New() *MapCache {
	return &MapCache{
		keys:  make(map[string]cacheEntry),
		epoch: Now(),
	}
}

func (mc *MapCache) Get(key string) any {
	entry, exists := mc.keys[key]
	if !exists {
		return nil
	}

	if entry.Expiration != 0 && entry.Expiration <= int32(Now()-mc.epoch) {
		delete(mc.keys, key)
		return nil
	}
	return entry.Value
}

func (mc *MapCache) Set(key string, value any, ttl time.Duration) {
	if ttl > 0 && ttl < time.Second {
		ttl = time.Second
	}
	expiration := int32(ttl.Seconds()) + int32(Now()-mc.epoch)
	mc.keys[key] = cacheEntry{Value: value, Expiration: expiration}
}

func (mc *MapCache) Delete(key string) {
	delete(mc.keys, key)
}

func (mc *MapCache) Reset() {
	mc.keys = make(map[string]cacheEntry)
	mc.epoch = Now()
}

func (mc *MapCache) Clean() {
	now := int32(Now() - mc.epoch)
	for key, entry := range mc.keys {
		if entry.Expiration <= now {
			delete(mc.keys, key)
		}
	}
}

func (mc *MapCache) NumKeys() int {
	return len(mc.keys)
}
