package mapcache_test

import (
	"testing"
	"time"

	"go.mukunda.com/oncache/internal/mapcache"
)

var nowRestore = mapcache.Now
var testTime int64
var testTimeBase = int64(1735689600) // 2025-01-01 00:00:00 UTC

func useTime(t int64) {
	testTime = t
	mapcache.Now = func() int64 {
		return testTimeBase + testTime
	}
}

func advanceTime(d int64) {
	testTime += d
}

func restoreTime() {
	mapcache.Now = nowRestore
}

func assert(t *testing.T, cond bool) {
	t.Helper()
	if !cond {
		t.Fatalf("Assertion failed.")
	}
}

func TestMapCache(t *testing.T) {
	useTime(0)
	defer restoreTime()

	// [SPEC] The mapcache doesn't have a key limit, but it has an expiration time on each
	//        key. When a key is accessed and it is expired, nil is returned and the key
	//        data is deleted.

	mc := mapcache.New()
	mc.Set("key1", "value1", time.Second*10)
	mc.Set("key2", "value2", time.Second*15)

	assert(t, mc.NumKeys() == 2)

	advanceTime(9)
	assert(t, mc.Get("key1") == "value1")
	assert(t, mc.Get("key2") == "value2")

	advanceTime(1)
	// [SPEC] Key data is not deleted until it is accessed.
	assert(t, mc.NumKeys() == 2)
	assert(t, mc.Get("key1") == nil)
	assert(t, mc.Get("key2") == "value2")
	assert(t, mc.NumKeys() == 1)

	advanceTime(5)
	assert(t, mc.Get("key2") == nil)
}

func TestMapCacheClean(t *testing.T) {
	useTime(0)
	defer restoreTime()
	// [SPEC] The Clean function deletes any expired keys immediately.

	mc := mapcache.New()
	mc.Set("key1", "value1", time.Second*10)
	mc.Set("key2", "value2", time.Second*10)
	mc.Set("key3", "value2", time.Second*11)

	advanceTime(9)
	mc.Clean()
	assert(t, mc.NumKeys() == 3)
	advanceTime(1)
	assert(t, mc.NumKeys() == 3)
	mc.Clean()
	assert(t, mc.NumKeys() == 1)
}

func TestMapCacheDelete(t *testing.T) {
	mc := mapcache.New()
	mc.Set("key1", "value1", time.Second*10)
	mc.Delete("key1")
	assert(t, mc.Get("key1") == nil)
}

func TestMapCacheNoExpiration(t *testing.T) {
	useTime(0)
	defer restoreTime()

	// [SPEC] When the expiration time is 0, the key never expires.
	mc := mapcache.New()
	mc.Set("key1", "value1", 0)
	advanceTime(86400 * 365)
	assert(t, mc.Get("key1") == "value1")
}

func TestMapCacheExpirationTime(t *testing.T) {
	useTime(0)
	defer restoreTime()

	// [SPEC] Expiration time smaller than 1 second is rounded up to 1 second.
	mc := mapcache.New()
	mc.Set("key1", "value1", 1)             // 1 second
	mc.Set("key2", "value2", time.Second+1) // 1 second
	assert(t, mc.Get("key1") == "value1")
	assert(t, mc.Get("key2") == "value2")

	advanceTime(1)
	assert(t, mc.Get("key1") == nil)
	assert(t, mc.Get("key2") == nil)
}
