package oncache_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"go.mukunda.com/oncache"
)

func assert(t *testing.T, cond bool) {
	t.Helper()
	if !cond {
		t.Fatal("assertion failed")
	}
}

func TestOncacheSingleNode(t *testing.T) {
	oncache.SetLogger(oncache.StdOutLogger)
	oc := oncache.New()
	oc.SetPort(8850)
	cache := oc.NewCache("test", oncache.NewCacheOptions{
		MaxKeys: 2,
	})

	oc.Init(make([]byte, 16), "127.0.0.1:8850")

	// [SPEC] The cache will only allow the specified amount of keys. Keys are evicted
	//        according to the underlying eviction strategy. The default strategy is SIEVE.

	cache.Set("key1", "key1")
	cache.Set("key2", "key2")
	cache.Set("key3", "key3")

	assert(t, cache.Get("key1") == nil)    // key1 should be evicted.
	assert(t, cache.Get("key2") == "key2") // set key2 as visited.
	cache.Set("key4", "key4")
	assert(t, cache.Get("key3") == nil) // key3 should be evicted.

	// [SPEC] Deleting from the oncache instance accepts the format <name>/<key>.
	oc.Delete("test/key4")
	assert(t, cache.Get("key4") == nil)
	cache.Set("key4", "key4")
	assert(t, cache.Get("key2") == "key2")
	assert(t, cache.Get("key4") == "key4")

	// [SPEC] If you omit the key part of an instance delete, then the entire cache is
	//        reset. This is equivalent to calling Reset on the cache.
	oc.Delete("test")
	assert(t, cache.Get("key2") == nil)
	assert(t, cache.Get("key4") == nil)
}

func TestOncacheCluster(t *testing.T) {
	nodes := createCluster(24)
	defer shutdownCluster(nodes)
	// key := []byte("1234567890123456")

	// // 24 nodes.
	// nodes := make([]*oncache.Oncache, 24)

	// //this may cause the test to fail from stdout spam slowing things down.
	// //oncache.SetLogger(oncache.StdOutLogger)

	// connects := []string{}
	// for i := range nodes {
	// 	nodes[i] = oncache.New()
	// 	nodes[i].SetPort(14250 + i)
	// 	host := "127.0.0.1"
	// 	nodes[i].Init(key, host)
	// 	hostWithPort := fmt.Sprintf("%s:%d", host, 14250+i)
	// 	connects = append(connects, hostWithPort)
	// }

	// // [SPEC] Nodes are provided with a list of hostnames to communicate with. Nodes ignore
	// //        their own hostname.
	// for i := range nodes {
	// 	nodes[i].Connect(connects)
	// }

	// For this test, we'll have QUICK access to a local database like this. Each node will
	// read and write randomly to this database. They can confirm that they are in sync by
	// checking the "updatedAt" time whenever there is an error. If a cache record doesn't
	// match a database record, and the database was recently updated, then ignore the
	// error. Otherwise, it indicates that something did not invalidate properly.
	database := sync.Map{}
	type record struct {
		value     string
		updatedAt time.Time
	}

	//dblock := sync.Mutex{}

	dbSet := func(key, value string) {
		database.Store(key, record{value, time.Now()})
	}

	dbGet := func(key string) record {
		v, ok := database.Load(key)
		if !ok {
			return record{"x", time.Now()}
		}
		return v.(record)
	}
	var halt sync.WaitGroup

	shutdown := make(chan struct{})

	getRandKey := func(dice *rand.Rand) string {
		return fmt.Sprintf("key%d", dice.Intn(200))
	}

	waitGroup := sync.WaitGroup{}
	oncache.ManagerLock = &sync.Mutex{}

	waitGroup.Add(len(nodes))
	for i := range nodes {
		go func(i int) {
			defer waitGroup.Done()

			cache := nodes[i].NewCache("test", oncache.NewCacheOptions{
				MaxKeys: 50,
			})
			dice := rand.New(rand.NewSource(int64(i)))

			// Simulate work in here.
			for {
				select {
				case <-shutdown:
					return
				case <-time.After(time.Duration(dice.Intn(10)) * time.Millisecond):
					halt.Wait()
					key := getRandKey(dice)
					if dice.Intn(2) == 0 {
						rec := cache.Get(key)
						if rec == nil {
							// Cache miss. Load from DB.
							oncache.ManagerLock.Lock()
							val := dbGet(key)
							cache.Set(key, val)
							oncache.ManagerLock.Unlock()
						} else {
							dbval := dbGet(key)
							if rec.(record).value != dbval.value {

								// Cache is outdated. We can accept some error to anticipate
								// cache invalidation time, but too much indicates an error.
								delayed := time.Since(dbval.updatedAt)
								t.Log("delay for mismatch =", delayed)
								if delayed > time.Millisecond*500 {
									panic(
										fmt.Sprintf("cache data too far outdated: %dms",
											delayed.Milliseconds()))
								}

								// We expect the cache to erase the key very shortly (reciving an
								// invalidation message)
								var j int

								// HALT all other threads to prevent them writing to this key
								// again. The DELETE message should already be in the network.
								halt.Add(1)
								for j = 0; j < 20; j++ {
									rec := cache.Get(key)
									if rec == nil {
										goto clearedKey
									}
									time.Sleep(time.Millisecond * 10)
								}
								panic("key did not clear in time")
							clearedKey:
								halt.Done()

							} else {
							}
						}
					} else {
						// Perform a data "update"
						newval := fmt.Sprintf("v-%d", dice.Intn(100))
						dbSet(key, newval)
						cache.Delete(key)
					}
				}
			}
		}(i)
	}

	time.Sleep(time.Second * 25)
	close(shutdown)
	waitGroup.Wait()
}

func createCluster(n int) []*oncache.Oncache {
	key := []byte("1234567890123456")

	nodes := make([]*oncache.Oncache, n)

	connects := []string{}
	for i := range nodes {
		nodes[i] = oncache.New()
		nodes[i].SetPort(14250 + i)
		host := "127.0.0.1"
		nodes[i].Init(key, host)
		hostWithPort := fmt.Sprintf("%s:%d", host, 14250+i)
		connects = append(connects, hostWithPort)
	}

	// [SPEC] Nodes are provided with a list of hostnames to communicate with. Nodes ignore
	//        their own hostname.
	for i := range nodes {
		nodes[i].Connect(connects)
	}

	return nodes
}

func shutdownCluster(nodes []*oncache.Oncache) {
	for i := range nodes {
		nodes[i].Shutdown()
	}
}

func TestOncacheClusterMessages(t *testing.T) {
	nodes := createCluster(6)
	defer shutdownCluster(nodes)

	nodes[0].Subscribe("test", func(host string, channel string, key string) {
		// todo
	})
}

// nodes := make([]*oncache.Oncache, 6)

// connects := []string{}
// for i := range nodes {
// 	nodes[i] = oncache.New()
// 	nodes[i].SetPort(14250 + i)
// 	host := "127.0.0.1"
// 	nodes[i].Init(key, host)
// 	hostWithPort := fmt.Sprintf("%s:%d", host, 14250+i)
// 	connects = append(connects, hostWithPort)
// }
