//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package oncache_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mukunda.com/oncache"
	"go.mukunda.com/oncache/sieve"
)

// Assert a condition is true or mark test as failed.
func assert(t *testing.T, cond bool) {
	t.Helper()
	if !cond {
		t.Error("assertion failed")
	}
}

// Poll for a condition to be true within a given timeout period.
func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	start := time.Now()
	for {
		if cond() {
			return
		}
		if time.Since(start) > timeout {
			t.Error("condition did not pass within timeout")
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func createCluster(n int) []*oncache.Oncache {
	// Create N nodes and connect them together.
	key := []byte("1234567890123456")

	nodes := make([]*oncache.Oncache, n)

	connects := []string{}

	setupTimeout := time.Now().Add(time.Second * 10)

	for i := range nodes {
		nodes[i] = oncache.New()
		nodes[i].SetPort(14250 + i)
		host := "127.0.0.1"
		nodes[i].Start(key, host)
		hostWithPort := fmt.Sprintf("%s:%d", host, 14250+i)
		connects = append(connects, hostWithPort)

		for !nodes[i].ListenerStarted() {
			if time.Now().After(setupTimeout) {
				panic(fmt.Sprintf("node %d did not start within timeout", i))
			}
			time.Sleep(time.Millisecond * 1)
		}
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
		nodes[i].Stop()
	}
}

func TestOncacheSingleNode(t *testing.T) {
	oncache.SetLogger(oncache.StdOutLogger)
	defer oncache.SetLogger(nil)

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
	oncache.PeerQueueSize = 10000
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

	shutdown := make(chan struct{})

	getRandKey := func(dice *rand.Rand) string {
		return fmt.Sprintf("key%d", dice.Intn(200))
	}

	waitGroup := sync.WaitGroup{}
	oncache.ManagerLock = &sync.Mutex{}

	waitGroup.Add(len(nodes))

	halter := sync.Mutex{}

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
					halter.Lock()
					halter.Unlock()
					//halt.Wait()
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
								halter.Lock()
								for j = 0; j < 20; j++ {
									rec := cache.Get(key)
									if rec == nil {
										goto clearedKey
									}
									time.Sleep(time.Millisecond * 10)
								}
								panic("key did not clear in time")
							clearedKey:
								halter.Unlock()

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
		if channel != "1" {
			assert(t, message == "TEST MESSAGE")
			assert(t, host == "127.0.0.1:14250")
			assert(t, channel == "🚀")
			nodes[1].DispatchMessage("👋", "TEST MESSAGE BOUNCE")
		}
	})

	nodes[0].Subscribe("", func(host string, channel string, message string) {
		if channel != "1" {
			assert(t, message == "TEST MESSAGE BOUNCE")
			assert(t, host == "127.0.0.1:14251")
			assert(t, channel == "👋")
			close(done)
		}
	})

	nodes[0].DispatchMessage("🚀", "TEST MESSAGE")

	<-done
}

func TestOncacheMessagePassingCluster(t *testing.T) {
	pqs := oncache.PeerQueueSize
	oncache.PeerQueueSize = 500
	defer func() {
		oncache.PeerQueueSize = pqs
	}()

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
			if channel != "1" {
				// Received message on any channel
				removeExpected(ni, "", message)
			}
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

func TestUnsubscribe(t *testing.T) {
	nodes := createCluster(2)
	defer shutdownCluster(nodes)

	// [SPEC] Subscribers can be removed by their channel and ID.

	wg := sync.WaitGroup{}
	wg.Add(4)

	should1and4getmessages := true

	id1 := nodes[1].Subscribe("", func(host string, channel string, message string) {
		if channel != "1" {
			assert(t, message == "x-323801")
			if !should1and4getmessages {
				t.Error("unexpected message")
			}
			wg.Done()
		}
	})

	id2 := nodes[1].Subscribe("", func(host string, channel string, message string) {
		if channel != "1" {
			assert(t, message == "x-323801")
			wg.Done()
		}
	})

	id3 := nodes[1].Subscribe("c1", func(host string, channel string, message string) {
		assert(t, message == "x-323801")
		if !should1and4getmessages {
			t.Error("unexpected message")
		}
		wg.Done()
	})

	id4 := nodes[1].Subscribe("c1", func(host string, channel string, message string) {
		assert(t, message == "x-323801")
		wg.Done()
	})

	// [SPEC] When the channel doesn't match an ID, it's a no-op.
	nodes[1].Unsubscribe("c1", id2)
	nodes[1].Unsubscribe("", id4)

	nodes[0].DispatchMessage("c1", "x-323801") //Will be received by all subs

	wg.Wait()

	nodes[1].Unsubscribe("", id1)
	nodes[1].Unsubscribe("c1", id3)
	should1and4getmessages = false

	for i := 0; i < 5; i++ {
		// Iterate this a few times just to make sure that those other subs do not receive
		// messages.
		wg.Add(2)
		nodes[0].DispatchMessage("c1", "x-323801") // Will be received by sub2 and sub4
		wg.Wait()
	}
}

type captureLogger struct {
	messages []string
}

func (c *captureLogger) Install() {
	oncache.SetLogger(func(level oncache.LogLevel, message string) {
		c.Log(level, message)
	})
}

func (c *captureLogger) Uninstall() {
	oncache.SetLogger(nil)
}

func (c *captureLogger) Log(level oncache.LogLevel, message string) {
	prefix := "[]"
	switch level {
	case oncache.LogLevelDebug:
		prefix = "[DEBUG]"
	case oncache.LogLevelInfo:
		prefix = "[INFO]"
	case oncache.LogLevelWarn:
		prefix = "[WARN]"
	case oncache.LogLevelError:
		prefix = "[ERROR]"
	}
	c.messages = append(c.messages, prefix+" "+message)
}

func (c *captureLogger) Count(matcher string) int {
	count := 0
	rx := regexp.MustCompile(matcher)
	for _, msg := range c.messages {
		if rx.FindString(msg) != "" {
			count++
		}
	}
	return count
}

func (c *captureLogger) Reset() {
	c.messages = []string{}
}

func TestBadDispatch(t *testing.T) {
	logger := captureLogger{}
	logger.Install()
	defer logger.Uninstall()

	// [SPEC] Dispatching a message with invalid parameters is ignored with a logged error.
	nodes := createCluster(1)
	defer shutdownCluster(nodes)

	// [SPEC] DispatchMessage with an empty channel is invalid.
	nodes[0].DispatchMessage("", "")
	assert(t, logger.Count("ERROR.*empty channel") == 1)
	logger.Reset()

	// [SPEC] DispatchMessage with a newline in either channel or message is invalid.
	nodes[0].DispatchMessage("\n", "")
	nodes[0].DispatchMessage("c1", "\n")
	nodes[0].DispatchMessage("\n", "\n")
	assert(t, logger.Count("ERROR.*line break") == 3)
}

func count(slice []string, item string) int {
	count := 0
	for _, s := range slice {
		if s == item {
			count++
		}
	}
	return count
}

func TestNewConnectingNode(t *testing.T) {
	oncache.SetLogger(oncache.StdOutLogger)
	defer oncache.SetLogger(nil)
	nodes := []*oncache.Oncache{}
	defer func() {
		shutdownCluster(nodes)
	}()

	// [SPEC] When a node receives a connection from another node, that other node is
	// registered as a peer.

	{
		node := oncache.New()
		node.SetPort(14250)
		node.Start([]byte("1234567890123456"), "127.0.0.1")
		nodes = append(nodes, node)
	}

	{
		node := oncache.New()
		node.SetPort(14251)
		// [SPEC] Omitting the port in the hostname will default to the listen port.
		node.Start([]byte("1234567890123456"), "127.0.0.1")
		nodes = append(nodes, node)
	}

	received := []string{}
	node1subbed := make(chan struct{})

	wg := sync.WaitGroup{}
	wg.Add(2)

	nodes[0].Subscribe("", func(host string, channel string, message string) {
		if message == "SUB" {
			// node1 has subbed.
			close(node1subbed)
		}
		if channel == "c1" {
			received = append(received, host+";"+channel+";"+message)
			if message == "message3" {
				wg.Done()
			} else {
				t.Error("unexpected message")
			}
		}
	})

	nodes[1].Subscribe("", func(host string, channel string, message string) {
		if channel == "c1" {
			received = append(received, host+";"+channel+";"+message)
			if message == "message2" {
				wg.Done()
			} else {
				t.Error("unexpected message")
			}
		}
	})

	// This message will go nowhere.
	nodes[0].DispatchMessage("c1", "message1")

	nodes[1].Connect([]string{"127.0.0.1:14250"})
	<-node1subbed

	// This message will reach nodes[1].
	nodes[0].DispatchMessage("c1", "message2")

	// This message will reach nodes[0].
	nodes[1].DispatchMessage("c1", "message3")

	wg.Wait()

	assert(t, len(received) == 2)
	assert(t, count(received, "127.0.0.1:14250;c1;message2") == 1)
	assert(t, count(received, "127.0.0.1:14251;c1;message3") == 1)
}

var nowRestore = sieve.Now
var testTime int64
var testTimeBase = int64(1735689600) // 2025-01-01 00:00:00 UTC

func useTime(t int64) {
	testTime = t
	sieve.Now = func() int64 {
		return testTimeBase + testTime
	}
}

func advanceTime(d int64) {
	testTime += d
}

func restoreTime() {
	sieve.Now = nowRestore
}

func TestClusterUpDownPorts(t *testing.T) {
	for i := 0; i < 100; i++ {
		nodes := createCluster(3)
		shutdownCluster(nodes)
	}
}

func TestKeyExpiration(t *testing.T) {
	useTime(0)
	defer restoreTime()

	nodes := createCluster(1)
	defer shutdownCluster(nodes)
	node := nodes[0]

	// [SPEC] The default expiration time is 5 minutes. Passing 0 as the expiration time
	// option will use the default.
	{
		cache := node.NewCache("test", oncache.NewCacheOptions{})
		cache.Set("key1", "value1")

		advanceTime(60*5 - 1)
		assert(t, cache.Get("key1") == "value1")
		advanceTime(1)
		assert(t, cache.Get("key1") == nil)
	}

	useTime(0)
	{
		cache2 := node.NewCache("test2", oncache.NewCacheOptions{
			DefaultExpiration: time.Second * 10,
		})
		cache2.Set("key1", "value1")

		advanceTime(9)
		assert(t, cache2.Get("key1") == "value1")
		advanceTime(1)
		assert(t, cache2.Get("key1") == nil)
	}

	useTime(0)
	{
		// [SPEC] Tiny values for NewCacheOptions.DefaultExpiration are rounded up to 1.
		cache3 := node.NewCache("test3", oncache.NewCacheOptions{
			DefaultExpiration: 1,
		})
		cache3.Set("key1", "value1")
		assert(t, cache3.Get("key1") == "value1")
		advanceTime(1)
		assert(t, cache3.Get("key1") == nil)

		// [SPEC] Keys can have their expiration set individually.
		cache3.SetEx("key2", 1, time.Second*10)
		advanceTime(9)
		assert(t, cache3.Get("key2") == 1)
		advanceTime(1)
		assert(t, cache3.Get("key2") == nil)

		// [SPEC] Negative values are treated the same as 0, e.g., never expires.
		cache3.SetEx("key3", 1, 0)
		cache3.SetEx("key4", 1, -1)
		assert(t, cache3.Get("key3") == 1)
		assert(t, cache3.Get("key4") == 1)
	}
}

func TestDeleteCache(t *testing.T) {
	// [SPEC] When Delete is called on a cache. All other nodes will reset the cache with
	// the same name.
	nodes := createCluster(2)
	defer shutdownCluster(nodes)

	c1 := nodes[0].NewCache("test")
	c2 := nodes[1].NewCache("test")

	c1.Set("key1", "value1")
	c2.Set("key2", "value2")

	nodes[0].Delete("test")

	waitFor(t, func() bool {
		return c1.Get("key1") == nil && c2.Get("key2") == nil
	}, time.Second*5)

	c1.Set("key1", "value1")
	c2.Set("key2", "value2")

	c1.Reset()

	waitFor(t, func() bool {
		return c1.Get("key1") == nil && c2.Get("key2") == nil
	}, time.Second*5)

	c1.Set("key1", "value1")
	c2.Set("key2", "value2")

	// [SPEC] ResetLocal only clears the local cache.

	c1.ResetLocal()
	time.Sleep(time.Millisecond * 250)
	assert(t, c1.Get("key1") == nil)
	assert(t, c2.Get("key2") == "value2")

	// [SPEC] Deleting a key that doesn't exist locally still deletes all instances of it
	// in the cluster.

	c2.Set("key2", "value2")

	c1.Delete("key2")

	waitFor(t, func() bool {
		return c2.Get("key2") == nil
	}, time.Second*5)
}

// type Bomb struct {
// 	boomer  chan int
// 	payload []byte
// }

// // Create an object that sends a signal on the given channel when it is garbage collected.
// func makeBomb(boomer chan int) *Bomb {
// 	bomb := &Bomb{boomer, make([]byte, 1000000)}
// 	runtime.SetFinalizer(bomb, func(b *Bomb) {
// 		b.boomer <- 1
// 	})
// 	return bomb
// }

func TestCacheCleaning(t *testing.T) {

	// Lower the polling period so it triggers within the test time quickly. We're also
	// using mocktime below.
	cpp := oncache.CleanupPollingPeriod
	oncache.CleanupPollingPeriod = time.Millisecond * 1
	defer func() {
		oncache.CleanupPollingPeriod = cpp
	}()

	useTime(0)
	defer restoreTime()

	nodes := createCluster(1)
	defer shutdownCluster(nodes)

	// [SPEC] Clean is called automatically if a cleanupPeriod is specified.
	// [SPEC] The minimum clean up period is 1 second, and smaller values will be rounded
	//        up.
	cache := nodes[0].NewCache("test", oncache.NewCacheOptions{
		CleanupPeriod: time.Millisecond * 1,
	})

	cache.SetEx("key1", 1, time.Second*10)
	advanceTime(10)
	time.Sleep(time.Millisecond * 200)

	waitFor(t, func() bool {
		return cache.NumKeys() == 0
	}, time.Second)
}

func TestCacheManualCleaning(t *testing.T) {

	// We want to make sure that the auto cleanup is not triggering during our test, as
	// it's disabled for the cache.
	cpp := oncache.CleanupPollingPeriod
	oncache.CleanupPollingPeriod = time.Millisecond * 1
	defer func() {
		oncache.CleanupPollingPeriod = cpp
	}()

	useTime(0)
	defer restoreTime()

	nodes := createCluster(1)
	defer shutdownCluster(nodes)

	// [SPEC] When the CleanupPeriod is NeverCleans, then the cache will never clean up
	//        expired records automatically. Clean can be called manually to trigger
	//        cleanup.

	cache := nodes[0].NewCache("test", oncache.NewCacheOptions{
		// Disable auto cleanup.
		CleanupPeriod: oncache.NeverCleans,
	})

	cache.SetEx("key1", 1, time.Second)
	advanceTime(10)
	time.Sleep(time.Millisecond * 200) // run auto-clean
	assert(t, cache.NumKeys() == 1)

	// Now clean it up manually.
	cache.Clean()
	assert(t, cache.NumKeys() == 0)
}

func TestCreateDuplicateCache(t *testing.T) {
	nodes := createCluster(1)
	defer shutdownCluster(nodes)

	logger := captureLogger{}
	logger.Install()
	defer logger.Uninstall()

	// [SPEC] Creating a cache with the same name will return the existing cache. Settings
	//        for the second creation are ignored.

	c1 := nodes[0].NewCache("test", oncache.NewCacheOptions{MaxKeys: 1})
	c2 := nodes[0].NewCache("test", oncache.NewCacheOptions{MaxKeys: 2})

	assert(t, c1 == c2)
	c1.Set("a", 1)
	c1.Set("b", 2)

	assert(t, c1.Get("a") == nil)
	assert(t, c1.Get("b") == 2)

	// [SPEC] An error is reported when trying to recreate a cache.
	logger.Count("ERROR.*already exists")
}

func TestDefaultExpiration(t *testing.T) {
	nodes := createCluster(1)
	defer shutdownCluster(nodes)

	useTime(0)
	defer restoreTime()

	// [SPEC] The default expiration time is 5 minutes.

}

func TestDialerTimeout(t *testing.T) {
	defer func(dt time.Duration) {
		oncache.DialerTimeout = dt
	}(oncache.DialerTimeout)
	oncache.DialerTimeout = time.Millisecond * 100

	// [SPEC] When a node is unreachable, the system will try to reach it over the
	// DialerTimeout period before giving up and removing the peer.

	nodes := createCluster(2)
	defer shutdownCluster(nodes)

	assert(t, nodes[0].NumPeers() == 1)

	nodes[1].Stop()

	nodes[0].DispatchMessage("test", "test")

	waitFor(t, func() bool {
		return nodes[0].NumPeers() == 0
	}, time.Second*2)
}

func TestListenerFailedStartRetryTime(t *testing.T) {
	defer func(dt time.Duration) {
		oncache.ListenerFailedStartRetryTime = dt
	}(oncache.ListenerFailedStartRetryTime)
	oncache.ListenerFailedStartRetryTime = time.Millisecond * 50

	logger := captureLogger{}
	logger.Install()
	defer logger.Uninstall()

	// [SPEC] When a node fails to start its listener, it will retry after
	//        ListenerFailedStartRetryTime.

	halter, _ := net.Listen("tcp", ":14250")
	go func() {
		time.Sleep(time.Millisecond * 100)
		halter.Close()
	}()

	// createCluster doesn't return until the listener starts successfully.
	nodes := createCluster(1)
	defer shutdownCluster(nodes)

	// [SPEC] An error is reported when the listener fails to start.
	assert(t, logger.Count("ERROR.*Failed to start listener") > 0)
}

func TestListenerCancelRestart(t *testing.T) {
	halter, _ := net.Listen("tcp", ":14250")
	defer halter.Close()

	logger := captureLogger{}
	logger.Install()
	defer logger.Uninstall()

	node := oncache.New()
	node.SetPort(14250)
	node.Start([]byte("1234567890123456"), "127.0.0.1")

	waitFor(t, func() bool {
		return logger.Count("ERROR.*Failed to start listener") > 0
	}, time.Second*1)

	startOfStop := time.Now()
	node.Stop()
	endOfStop := time.Now()

	// [SPEC] When the stop signal is raised, faulted processes exit immediately.
	assert(t, endOfStop.Sub(startOfStop) < time.Second)
}

func TestStartStopImmediate(t *testing.T) {
	c := oncache.New()
	c.SetPort(14250)
	c.Start([]byte("1234567890123456"), "127.0.0.1")

	// [SPEC] The Stop function should be robust and allow stopping before the init
	// process even completes.
	startOfStop := time.Now()
	c.Stop()
	endOfStop := time.Now()
	assert(t, endOfStop.Sub(startOfStop) < time.Second)
}

func TestDoubleStart(t *testing.T) {
	nodes := createCluster(2)
	defer shutdownCluster(nodes)

	logger := captureLogger{}
	logger.Install()
	defer logger.Uninstall()

	// [SPEC] If Start is called more than once during the lifetime, an error is logged and
	// the call is ignored.

	nodes[0].Start([]byte("3333333333333333"), "127.0.0.1")
	nodes[0].Start([]byte("3333333333333333"), "127.0.0.1")

	assert(t, logger.Count("ERROR.*initialized") == 2)

	// We want to make sure that the system is still functioning normally after those two
	// bogus Start calls.
	exit := make(chan int)

	nodes[0].Subscribe("test", func(host string, channel string, message string) {
		exit <- 1
	})

	nodes[1].DispatchMessage("test", "test")

	<-exit
}

func TestBadKey(t *testing.T) {
	// [SPEC] Oncache will reject Start if the key is invalid.

	for i := 0; i < 100; i++ {
		if i != 16 && i != 24 && i != 32 {
			node := oncache.New()
			node.SetPort(14250)
			assert(t,
				errors.Is(node.Start(bytes.Repeat([]byte{0}, i), "127.0.0.1"),
					oncache.ErrInvalidKey))
		}
	}
}

func TestListenerCrash(t *testing.T) {
	// [SPEC] If the listener crashes, the system should attempt to restart it.
	oncache.SetDebugListenerCrash(true)
	defer oncache.SetDebugListenerCrash(false)

	logger := captureLogger{}
	logger.Install()
	defer logger.Uninstall()

	nodes := createCluster(1)
	defer shutdownCluster(nodes)

	time.Sleep(time.Millisecond * 100)

	assert(t, logger.Count("ERROR.*listener.*panic") > 0)

	// This should cancel the restart immediately.
	startOfStop := time.Now()
	nodes[0].Stop()
	endOfStop := time.Now()
	assert(t, endOfStop.Sub(startOfStop) < time.Second)
}

func TestListenerCrashRecover(t *testing.T) {
	defer func(prc time.Duration) {
		oncache.ProcessPanicRecoveryDelay = prc
	}(oncache.ProcessPanicRecoveryDelay)
	oncache.ProcessPanicRecoveryDelay = time.Millisecond * 5

	oncache.SetDebugListenerCrash(true)
	defer oncache.SetDebugListenerCrash(false)

	logger := captureLogger{}
	logger.Install()
	defer logger.Uninstall()

	node := oncache.New()
	node.SetPort(14250)
	node.Start([]byte("1234567890123456"), "127.0.0.1")
	defer node.Stop()

	time.Sleep(time.Millisecond * 100)

	assert(t, logger.Count("ERROR.*listener.*panic") > 0)

	// OK, the listener is panicking. Let's recover it.

	// [SPEC] The system will repeatedly try to recover failing processes.

	oncache.SetDebugListenerCrash(false)
	waitFor(t, func() bool {
		return node.ListenerStarted()
	}, time.Second*1)
}
