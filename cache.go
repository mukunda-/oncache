//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package oncache

import (
	"fmt"
	"sync"
	"time"

	"go.mukunda.com/oncache/sieve"
)

const NeverExpires = time.Duration(-1)
const NeverCleans = time.Duration(-1)
const DefaultExpiration = 5 * time.Minute
const DefaultCleanupPeriod = 10 * time.Minute
const DefaultMaxKeys = 1000

var ManagerLock *sync.Mutex = nil

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
	DeleteLocal(key string)

	// DeleteLocal and propagate to other nodes. Equivalent to oncache.Delete("name/key").
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

	// Discard all keys.
	Reset()

	// Discard all keys in this local cache only (do not propagate to other nodes).
	ResetLocal()
}

type dcache struct {
	parent            *Oncache
	name              string
	data              *sieve.Sieve
	maxKeys           int
	defaultExpiration int32 // seconds
	cleanupPeriod     int32 // seconds
	mutex             sync.RWMutex
	nextCleanup       Unixtime

	// Unixtime when this was created.
	// cacheValue expiration time is "epoch + cv.expiration"
	epoch int64
}

// No logging is done in Get/Set for performance reasons.

func (c *dcache) Get(key string) any {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.data.Get(key)
}

// Set a key with a specific expiration time. 0 or NeverExpires for no expiration.
func (c *dcache) SetEx(key string, value any, expiration time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if value == nil {
		c.data.Delete(key)
		return
	}

	if expiration < 0 {
		expiration = 0
	}
	c.data.Set(key, value, expiration)
}

// Set a key with the default expiration time.
func (c *dcache) Set(key string, value any) {
	c.SetEx(key, value, time.Duration(c.defaultExpiration)*time.Second)
}

// Delete (invalidate) a key from the local cache only (usually not desired).
func (c *dcache) DeleteLocal(key string) {
	c.Set(key, nil)
}

// Delete (invalidate) a key from the cache.
func (c *dcache) Delete(key string) {
	c.parent.Delete(c.name + "/" + key)
}

// Discard all keys from the local cache only (usually not desired).
func (c *dcache) ResetLocal() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.data.Reset()
}

// Discard all keys.
func (c *dcache) Reset() {
	c.ResetLocal()
	c.parent.Delete(c.name)
}

// Checks if the cleanup is due and then executes Clean.
func (c *dcache) checkAndRunClean() bool {
	if c.cleanupPeriod == 0 {
		// Auto cleanup is disabled.
		return false
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	if time.Now().Unix() < c.nextCleanup {
		return false
	}

	c.nextCleanup = Unixtime(time.Now().Unix()) + int64(c.cleanupPeriod)
	c.data.Clean()
	return true
}

func (c *dcache) Clean() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.data.Clean()

	/*
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
	*/
}

// Options for NewCache. All options can be `nil` which indicates the default value.
type NewCacheOptions struct {
	// The max amount of keys the cache can hold. Must be 1-65535. Default is 1000.
	// Currently there is no option to have "unlimited" keys, but an additional
	// implementation may support that in the future.
	MaxKeys int

	// The default period before values are considered stale.
	// -1 = never expire. 0 = use default (5 mins)
	DefaultExpiration time.Duration

	// The period between garbage collection. -1 = no cleanup, where expired keys will only
	// be deleted when they are naturally evicted. [Cache.Clean] can also be called
	// manually on cache instances. 0 = use default (10mins)
	CleanupPeriod time.Duration
}

// Create a new cache referenced by `name`.
//
// When referencing keys from this cache from functions such as Invalidate, prefix the
// key with "<name>/", e.g., mycache/mykey
//
// If the cache already exists, the existing cache is returned and an error is logged.
func (oc *Oncache) NewCache(name string, options ...NewCacheOptions) Cache {
	existing := oc.GetCache(name)
	if existing != nil {
		logError("Tried to create cache \"" + name + "\" which already exists; using existing cache.")
		return existing
	}

	cache := &dcache{
		parent:            oc,
		name:              name,
		maxKeys:           DefaultMaxKeys,
		defaultExpiration: int32(DefaultExpiration.Seconds()),
		cleanupPeriod:     int32(DefaultCleanupPeriod.Seconds()),
	}

	for _, opt := range options {
		if opt.MaxKeys != 0 {
			cache.maxKeys = opt.MaxKeys
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

	cache.data = sieve.NewSieve(uint16(cache.maxKeys))
	cache.nextCleanup = Unixtime(time.Now().Unix()) + int64(cache.cleanupPeriod)
	oc.registerCache(name, cache)
	return cache
}
