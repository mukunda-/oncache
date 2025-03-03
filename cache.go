//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package oncache

import (
	"fmt"
	"sync"
	"time"

	"go.mukunda.com/oncache/internal/mapchain"
)

const NeverExpires = time.Duration(-1)
const NeverCleans = time.Duration(-1)
const DefaultExpiration = 5 * time.Minute
const DefaultCleanupPeriod = 10 * time.Minute

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

	// Delete any expired keys. This is called automatically if the cache is configured
	// with a cleanupPeriod.
	Clean()
}

type dcache struct {
	parent            *Oncache
	name              string
	data              *mapchain.Mapchain
	maxKeys           int
	defaultExpiration int32 // seconds
	cleanupPeriod     int32 // seconds
	mutex             sync.RWMutex
	nextCleanup       Unixtime

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

func (c *dcache) checkAndRunClean() bool {
	if c.cleanupPeriod == 0 {
		return false
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if time.Now().Unix() < c.nextCleanup {
		return false
	}

	c.nextCleanup = Unixtime(time.Now().Unix()) + int64(c.cleanupPeriod)
	c.Clean()
	return true
}

func (c *dcache) Clean() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	now := time.Now().Unix()

	filter := mapchain.NewFilterJob(c.data, func(value any) bool {
		if value == nil {
			return true
		}

		exp := value.(cacheValue).expiration
		if exp == 0 {
			return false
		}

		return now > c.epoch+int64(exp)
	})

	go func() {

		for {
			_, _, done := filter.Run()
			if done {
				return
			}
			select {
			case <-time.After(100 * time.Millisecond):
				// continue
			case <-c.parent.shutdownSignal.C:
				return
			}
		}
	}()
}

// Options for NewCache. All options can be `nil` which indicates the default value.
type NewCacheOptions struct {
	// The max amount of keys the cache can hold. -1 = no limit, 0 = default (no limit)
	MaxKeys int

	// The default period before values are considered stale.
	// -1 = never expire. 0 = default (5 mins)
	DefaultExpiration time.Duration

	// The period between garbage collection. -1 = no cleanup (expired keys will never be
	// freed). No cleanup is useful if you have a set amount of possible keys.
	// [Cache.Clean] can also be called manually on cache instances. 0 = default (10mins)
	CleanupPeriod time.Duration
}

// Create a new cache referenced by `name`.
//
// When referencing keys from this cache from functions such as Invalidate, prefix the
// key with "<name>/", e.g., mycache/mykey
//
// If the cache already exists, the existing cache is returned and an error is logged.
func (oc *Oncache) NewCache(name string, options ...NewCacheOptions) Cache {
	cache := oc.GetCache(name).(*dcache)
	if cache != nil {
		logError("Tried to create cache \"" + name + "\" which already exists; using existing cache.")
		return cache
	}

	cache = &dcache{
		name:              name,
		data:              mapchain.NewMapChain(),
		maxKeys:           0,
		defaultExpiration: int32(DefaultExpiration.Seconds()),
		cleanupPeriod:     int32(DefaultCleanupPeriod.Seconds()),
	}

	for _, opt := range options {
		if opt.MaxKeys != 0 {
			cache.maxKeys = opt.MaxKeys
			if cache.maxKeys < 0 {
				cache.maxKeys = 0
			}
		}
		if opt.DefaultExpiration != 0 {
			cache.defaultExpiration = int32(opt.DefaultExpiration.Seconds())
			if cache.defaultExpiration < 0 {
				cache.defaultExpiration = 0
			}
		}
		if opt.CleanupPeriod != 0 {
			cache.cleanupPeriod = int32(opt.CleanupPeriod.Seconds())
			if cache.cleanupPeriod < 0 {
				cache.cleanupPeriod = 0
			}
		}
	}

	logInfo(
		fmt.Sprintf("Creating cache \"%s\"; maxkeys=%d; defaultexp=%d; cleanupPeriod=%d",
			name,
			cache.maxKeys,
			cache.defaultExpiration,
			cache.cleanupPeriod+cache.cleanupPeriod,
		))

	cache.nextCleanup = Unixtime(time.Now().Unix()) + int64(cache.cleanupPeriod)
	oc.registerCache(name, cache)
	return cache
}
