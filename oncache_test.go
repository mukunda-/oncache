package oncache_test

import (
	"fmt"
	"math/rand"
	"net"
	"strings"
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

	oc.Start(make([]byte, 16), "127.0.0.1:8850")

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
		nodes[i].Start(key, host)
		hostWithPort := fmt.Sprintf("%s:%d", host, 14250+i)
		connects = append(connects, hostWithPort)
	}

	// [SPEC] Nodes are provided with a list of hostnames to communicate with. Nodes ignore
	//        their own hostname.
	for i := range nodes {
		nodes[i].Connect(connects)
	}

	setupTimeout := time.Now().Add(time.Second * 10)
	for i := range nodes {
		for !nodes[i].ListenerStarted() {
			if time.Now().After(setupTimeout) {
				panic(fmt.Sprintf("node %d did not start within timeout", i))
			}
			time.Sleep(time.Millisecond * 1)
		}
	}

	return nodes
}

func shutdownCluster(nodes []*oncache.Oncache) {
	for i := range nodes {
		nodes[i].Stop()
	}
}

func TestOncacheCustomMessage(t *testing.T) {
	nodes := createCluster(2)
	defer shutdownCluster(nodes)

	done := make(chan struct{}, 5)
	fail := make(chan string)

	nodes[1].Subscribe("test1", func(host string, channel string, message string) {
		t.Log("got msg", message)
		if channel != "test1" {
			fail <- "unexpected channel " + channel + " in test1 handler"
		}
		if !strings.Contains(message, "TEST") {
			fail <- "message does not contain TEST"
		}
		if strings.Contains(message, "done") {
			done <- struct{}{}
		}
	})

	// [SPEC] When a message is dispatched, every other node receives it.

	nodes[0].DispatchMessage("test1", "TEST")
	nodes[0].DispatchMessage("test2", "TEST 1")
	nodes[0].DispatchMessage("test1", "TEST done")
	nodes[0].DispatchMessage("test1", "TEST 1")

	select {
	case <-time.After(time.Millisecond * 200):
		t.Fatal("timeout")
	case msg := <-fail:
		t.Fatal(msg)
	case <-done:
	}

	// [SPEC] Nodes do not receive their own messages.

	nodes[0].DispatchMessage("test1", "TEST")
	nodes[0].DispatchMessage("test2", "TEST 2")
	nodes[0].DispatchMessage("test1", "TEST 1")
	nodes[1].DispatchMessage("test1", "TEST done")

	select {
	case <-time.After(time.Millisecond * 200):
	case msg := <-fail:
		t.Fatal(msg)
	case <-done:
		t.Fatal("unexpected done")
	}
}

func TestSendAMessage(t *testing.T) {
	// Basic test to confirm message passing from A to B is a success.
	nodes := createCluster(2)
	defer shutdownCluster(nodes)

	done := make(chan struct{})

	// node0 -> TEST MESSAGE -> node1
	// node1 -> TEST MESSAGE BOUNCE -> node0

	nodes[1].Subscribe("", func(host string, channel string, message string) {
		assert(t, message == "TEST MESSAGE")
		assert(t, host == "127.0.0.1:14250")
		assert(t, channel == "🚀")
		nodes[1].DispatchMessage("👋", "TEST MESSAGE BOUNCE")
	})

	nodes[0].Subscribe("", func(host string, channel string, message string) {
		assert(t, message == "TEST MESSAGE BOUNCE")
		assert(t, host == "127.0.0.1:14251")
		assert(t, channel == "👋")
		close(done)
	})

	nodes[0].DispatchMessage("🚀", "TEST MESSAGE")

	<-done
}

func TestOncacheMessagePassingCluster(t *testing.T) {
	//oncache.SetLogger(oncache.StdOutLogger)
	nodes := createCluster(10)
	defer shutdownCluster(nodes)
	rnd := rand.New(rand.NewSource(4))

	// For this test, each node sends 100x3 messages. We verify that each message is
	// received by every other node.

	expected := make(map[string]struct{})
	expectedLock := sync.Mutex{}

	wg := sync.WaitGroup{}
	wg.Add(len(nodes))

	addExpected := func(fromNode int, channel string, message string) {
		t.Helper()
		expectedLock.Lock()
		defer expectedLock.Unlock()

		for n := range nodes {
			if n != fromNode {
				expected[fmt.Sprintf("to=%d chan=%s msg=%s", n, channel, message)] = struct{}{}
				expected[fmt.Sprintf("to=%d chan= msg=%s", n, message)] = struct{}{}
				wg.Add(2)
			}
		}
	}

	removeExpected := func(nodeIndex int, channel string, message string) {
		t.Helper()
		expectedLock.Lock()
		defer expectedLock.Unlock()
		key := fmt.Sprintf("to=%d chan=%s msg=%s", nodeIndex, channel, message)
		_, ok := expected[key]
		if !ok {
			t.Error("missing expected message: " + key)
		}
		delete(expected, key)
		wg.Done()
	}

	for nodeIndex, node := range nodes {
		ni := nodeIndex
		node.Subscribe("a", func(host string, channel string, message string) {
			// Received message on channel A
			removeExpected(ni, "a", message)
		})
		node.Subscribe("b", func(host string, channel string, message string) {
			// Received message on channel B
			removeExpected(ni, "b", message)
		})
		node.Subscribe("c", func(host string, channel string, message string) {
			// Received message on channel C
			removeExpected(ni, "c", message)
		})
		node.Subscribe("", func(host string, channel string, message string) {
			// Received message on any channel
			removeExpected(ni, "", message)
		})
	}

	for nodeIndex, node := range nodes {
		go func(nodeIndex int, node *oncache.Oncache) {
			for i := 0; i < 100; i++ {
				addExpected(nodeIndex, "a", fmt.Sprintf("%d-a%d", nodeIndex, i))
				addExpected(nodeIndex, "b", fmt.Sprintf("%d-b%d", nodeIndex, i))
				addExpected(nodeIndex, "c", fmt.Sprintf("%d-c%d", nodeIndex, i))
				node.DispatchMessage("a", fmt.Sprintf("%d-a%d", nodeIndex, i))
				node.DispatchMessage("b", fmt.Sprintf("%d-b%d", nodeIndex, i))
				node.DispatchMessage("c", fmt.Sprintf("%d-c%d", nodeIndex, i))
				time.Sleep(time.Microsecond*200 + time.Millisecond*time.Duration(rnd.Intn(3)))
			}
			wg.Done()
		}(nodeIndex, node)
	}

	wg.Wait()

	if len(expected) > 0 {
		t.Error("test integrity failed; length should end as zero")
	}
}

func TestNetListenBehavior(t *testing.T) {
	// Just want to make sure that net.Listen is what opens the port (and not
	// listener.Accept).
	listener, _ := net.Listen("tcp", "127.0.0.1:22345")
	defer listener.Close()

	conn, err := net.Dial("tcp", "127.0.0.1:22345")
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()
}
