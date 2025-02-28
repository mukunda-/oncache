//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package oncache

import (
	"sync"
	"time"

	"go.mukunda.com/oncache/internal/mapchain"
)

const NeverExpires = time.Duration(0)

type Cache interface {
	// Get a value from the cache. Returns nil if there is no value set or if the value has
	// gone stale. Separate sub-cache paths with slashes, e.g., "aaa/bbb/ccc"
	Get(key string) any

	// Set a value in the cache. `nil` will remove values.
	Set(key string, value any)

	// Delete a key from the cache. Equivalent to `Set(key, nil)`. If a path is a prefix
	// for a subpath, the subpath will be deleted as well.
	//
	// For example, if you have the keys
	//
	//   - aaa/bbb/key1
	//   - aaa/bbb/key2
	//
	// and you Delete("aaa"), both keys will be deleted.
	Delete(key string)

	// Set a value in the cache with a custom expiration time. Normally, a default
	// expiration time is used (specified when creating the cache).
	//
	// Specify zero or oncache.NeverExpires to create a permanent key. NeverExpires entries
	// may still be deleted if the cache is full and entries are evicted.
	SetEx(key string, value any, expiration time.Duration)
}

type dcache struct {
	name              string
	data              *mapchain.Mapchain
	maxKeys           int
	defaultExpiration int32
	cleanupPeriod     time.Duration
	mutex             sync.RWMutex

	// Unixtime when this was created.
	// cacheValue expiration time is "epoch + cv.expiration"
	epoch int64
}

type cacheValue struct {
	value      any
	expiration int32
}

// No logging is done in Get/Set for performance reasons.

func (c *dcache) Get(key string) any {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	value := c.data.Get(key)
	if value == nil {
		return nil
	}

	if time.Now().Unix() > c.epoch+int64(value.(cacheValue).expiration) {
		return nil
	}

	return value
}

func (c *dcache) SetEx(key string, value any, expiration time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if value == nil {
		c.data.Set(key, nil)
		return
	} else {
		c.data.Set(key, cacheValue{
			value:      value,
			expiration: int32(expiration.Seconds()),
		})
	}
}

func (c *dcache) Set(key string, value any) {
	c.SetEx(key, value, time.Duration(c.defaultExpiration)*time.Second)
}

func (c *dcache) Delete(key string) {
	c.Set(key, nil)
}

type NewCacheOptions struct {
	// -1 = no limit
	MaxKeys int

	// The default period before values are considered stale.
	DefaultStaleTime time.Duration

	// The period between garbage collection.
	CleanupPeriod time.Duration
}

// Create a new cache. This will panic if the name is already taken. When referencing keys
// from this cache from other functions such as Invalidate, prefix the key with "<name>/",
// e.g., mycache/mykey"
func New(name string, options NewCacheOptions) Cache {
	cache := dcache{
		name:              name,
		data:              mapchain.NewMapChain(),
		maxKeys:           options.MaxKeys,
		defaultExpiration: int32(options.DefaultStaleTime.Seconds()),
		cleanupPeriod:     options.CleanupPeriod,
	}

	logInfo("Created cache " + name)
	registerCache(name, &cache)
	return &cache
}
