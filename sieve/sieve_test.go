//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package sieve_test

import (
	"container/list"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"go.mukunda.com/oncache/sieve"
)

// We'll test against an alternate implementation.
type tSieve struct {
	List    list.List
	Keys    map[string]*list.Element
	Hand    *list.Element
	MaxKeys int
}

type tElement struct {
	key     string
	value   int
	expires int64
	visited bool
}

func newTestSieve(maxKeys int) *tSieve {
	return &tSieve{
		Keys:    make(map[string]*list.Element),
		MaxKeys: maxKeys,
	}
}

func (ts *tSieve) Set(key string, value int, ttl time.Duration) {
	elem := ts.Keys[key]
	if elem != nil {

		elem.Value.(*tElement).value = value
		if ttl != 0 {
			elem.Value.(*tElement).expires = sieve.Now() + int64(ttl.Seconds())
			if ts.Hand == elem {
				ts.Hand = ts.Hand.Prev()
			}
			ts.List.MoveToFront(elem)
		} else {
			elem.Value.(*tElement).expires = 0
		}

		return
	}

	if ts.List.Len() >= ts.MaxKeys {
		ts.evict()
	}
	expires := sieve.Now() + int64(ttl.Seconds())
	if ttl == 0 {
		expires = 0
	}
	ts.Keys[key] = ts.List.PushFront(&tElement{key, value, expires, false})
}

func (ts *tSieve) Get(key string) int {
	elem, ok := ts.Keys[key]
	if !ok {
		return -1
	}

	if elem.Value.(*tElement).expires != 0 && sieve.Now() >= elem.Value.(*tElement).expires {
		ts.discard(elem)
		return -1
	}

	elem.Value.(*tElement).visited = true
	return elem.Value.(*tElement).value
}

func (ts *tSieve) evict() {
	{
		// Additional implementation details: remove tail if it's expired. As per [7.3] in
		// the paper, the SIEVE algorithm is TTL-friendly because it maintains objects in
		// insertion order. If all keys have the same TTL, then the tail will always expire
		// first.
		o := ts.List.Back()
		if o == nil {
			panic("evict on empty list")
		}
		if o.Value.(*tElement).expires != 0 && sieve.Now() >= o.Value.(*tElement).expires {
			ts.discard(o)
			return
		}
	}

	if ts.Hand == nil {
		ts.Hand = ts.List.Back()
	}

	for ts.Hand.Value.(*tElement).visited &&
		(ts.Hand.Value.(*tElement).expires == 0 || sieve.Now() < ts.Hand.Value.(*tElement).expires) {
		ts.Hand.Value.(*tElement).visited = false
		ts.Hand = ts.Hand.Prev()
		if ts.Hand == nil {
			ts.Hand = ts.List.Back()
		}
	}

	ts.discard(ts.Hand)
}

func (ts *tSieve) discard(elem *list.Element) {
	if ts.Hand == elem {
		ts.Hand = elem.Prev()
	}
	ts.List.Remove(elem)
	delete(ts.Keys, elem.Value.(*tElement).key)
}

func (ts *tSieve) Clean() {
	for e := ts.List.Front(); e != nil; {
		next := e.Next()
		if e.Value.(*tElement).expires != 0 && sieve.Now() >= e.Value.(*tElement).expires {
			ts.discard(e)
		}
		e = next
	}
}

func assert(t *testing.T, cond bool) {
	t.Helper()
	if !cond {
		t.Fatalf("Assertion failed.")
	}
}

func TestSieveBasic(t *testing.T) {
	s := sieve.NewSieve(3)

	// [SPEC] When no expiration is used, the cache follows the SIEVE algorithm directly.

	// []
	//    ↑ (hand)
	assert(t, s.Get("a") == nil)
	s.Set("a", 1, 0)
	// [a]
	//     ↑
	assert(t, s.Get("b") == nil)
	s.Set("b", 2, 0)
	// [b, a]
	//        ↑
	assert(t, s.Get("c") == nil)
	s.Set("c", 3, 0)
	// [c, b, a]
	//           ↑

	// [c, b, ^a] ^ = visited
	//            ↑
	assert(t, s.Get("a").(int) == 1)

	assert(t, s.Get("d") == nil)
	s.Set("d", 4, 0)
	// [d, c, ^a]
	//         ↑ skip visited
	// [c, b, a]
	//     ↑ evict b
	// [d, c, a]
	//     ↑

	assert(t, s.Get("a") == 1)
	assert(t, s.Get("b") == nil)
	// [d, c, a^]
	//     ↑

	assert(t, s.Get("e") == nil)
	s.Set("e", 5, 0)
	assert(t, s.Get("c") == nil) // evicted
	// [e, d, a^]
	//     ↑

	assert(t, s.Get("a") == 1)
	assert(t, s.Get("d") == 4)
	assert(t, s.Get("e") == 5)
	// [e^, d^, a^]
	//      ↑

	assert(t, s.Get("f") == nil)
	s.Set("f", 6, 0)

	// [e^, d^, a^]
	//      ↑
	// [e^, d, a^]
	//  ↑
	// [e, d, a^]
	//        ↑
	// [e, d, a]
	//     ↑
	// [f, e, a]
	//     ↑
	assert(t, s.Get("d") == nil) // evicted

	assert(t, s.Get("a") == 1)
	assert(t, s.Get("e") == 5)
	assert(t, s.Get("f") == 6)
}

func TestTestSieveBasic(t *testing.T) {
	// Testing the same as above on the test sieve.
	s := newTestSieve(3)

	assert(t, s.Get("a") == -1)
	s.Set("a", 1, 0)
	assert(t, s.Get("b") == -1)
	s.Set("b", 2, 0)
	assert(t, s.Get("c") == -1)
	s.Set("c", 3, 0)
	assert(t, s.Get("a") == 1)
	assert(t, s.Get("d") == -1)
	s.Set("d", 4, 0)
	assert(t, s.Get("a") == 1)
	assert(t, s.Get("b") == -1)
	assert(t, s.Get("e") == -1)
	s.Set("e", 5, 0)
	assert(t, s.Get("c") == -1)
	assert(t, s.Get("a") == 1)
	assert(t, s.Get("d") == 4)
	assert(t, s.Get("e") == 5)
	assert(t, s.Get("f") == -1)
	s.Set("f", 6, 0)
	assert(t, s.Get("d") == -1)
	assert(t, s.Get("a") == 1)
	assert(t, s.Get("e") == 5)
	assert(t, s.Get("f") == 6)
}

func TestSieveRandomSetBatchNoExpiration(t *testing.T) {

	for i := 0; i < 2000; i++ {
		ts := newTestSieve(10)
		rs := sieve.NewSieve(10)

		for j := 0; j < 100; j++ {
			key := fmt.Sprintf("key%d", rand.Intn(20))
			value := rand.Intn(10)
			ts.Set(key, value, 0)
			rs.Set(key, value, 0)
		}

		for j := 0; j < 20; j++ {
			key := fmt.Sprintf("key%d", j)
			val := ts.Get(key)
			rval := rs.Get(key)
			if val != -1 {
				if val != rval {
					t.Fatalf("Mismatched value for key %s: %d, expected %d", key, rval, val)
				}
			} else {
				if rval != nil {
					t.Fatalf("Mismatched value for key %s: %d, expected %d", key, rval, val)
				}
			}
		}
	}
}

func TestSieveRandomCacheBatchNoExpiration(t *testing.T) {
	// Testing cache emulation of random 20 keys with 10 cache slots.
	// Each operation is GET attempt with following SET if not found.

	for i := 0; i < 2000; i++ {
		ts := newTestSieve(10)
		rs := sieve.NewSieve(10)

		for j := 0; j < 100; j++ {
			item := rand.Intn(20)
			key := fmt.Sprintf("key%d", item)

			tsget := ts.Get(key)
			if tsget != -1 {
				assert(t, tsget == item)
			} else {
				ts.Set(key, item, 0)
			}

			rsget := rs.Get(key)
			if rsget != nil {
				assert(t, tsget != -1)
				assert(t, rsget == item)
				assert(t, rsget.(int) == tsget)
			} else {
				rs.Set(key, item, 0)
			}
		}

		for j := 0; j < 20; j++ {
			key := fmt.Sprintf("key%d", j)
			val := ts.Get(key)
			rval := rs.Get(key)
			if val != -1 {
				if val != rval {
					t.Fatalf("Mismatched value for key %s: %d, expected %d", key, rval, val)
				}
			} else {
				if rval != nil {
					t.Fatalf("Mismatched value for key %s: %d, expected %d", key, rval, val)
				}
			}
		}
	}
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

func TestSieveTTLBasics(t *testing.T) {
	useTime(0)
	defer restoreTime()

	// [SPEC] If a key is expired, then it is removed from the list when an access is
	//        attempted. Nil is returned.

	ts := sieve.NewSieve(3)
	ts.Set("a", 1, 1*time.Second)
	assert(t, ts.Get("a") == 1)
	advanceTime(1)
	assert(t, ts.Get("a") == nil)
	ts.Set("a", 1, 2*time.Second)
	advanceTime(1)
	assert(t, ts.Get("a") == 1)
	advanceTime(1)
	assert(t, ts.Get("a") == nil)

	// [SPEC] When evicting keys, the tail is checked for expiration before the normal
	//        SIEVE algorithm.

	ts.Reset()
	ts.Set("a", 1, 20*time.Second) // A at the tail.
	advanceTime(5)
	ts.Set("b", 2, 20*time.Second) // B in the middle.
	advanceTime(5)
	ts.Set("c", 3, 20*time.Second) // C in the front.

	ts.Get("a")                    // Bump A.
	ts.Set("d", 4, 20*time.Second) // D will evict B, putting the hand in the middle. A  is skipped (visited).
	assert(t, ts.Get("b") == nil)

	advanceTime(10) // A is expired now, so A at the tail will be evicted, and not C.
	ts.Set("e", 5, 20*time.Second)
	assert(t, ts.Get("a") == nil)
	assert(t, ts.Get("c") == 3)
	assert(t, ts.Get("d") == 4)
	assert(t, ts.Get("e") == 5)
}

func TestTestSieveTTLBasics(t *testing.T) {
	useTime(0)
	defer restoreTime()

	// [SPEC] If a key is expired, then it is removed from the list when an access is
	//        attempted. Nil is returned.

	ts := newTestSieve(3)
	ts.Set("a", 1, 1*time.Second)
	assert(t, ts.Get("a") == 1)
	advanceTime(1)
	assert(t, ts.Get("a") == -1)
	ts.Set("a", 1, 2*time.Second)
	advanceTime(1)
	assert(t, ts.Get("a") == 1)
	advanceTime(1)
	assert(t, ts.Get("a") == -1)

	// [SPEC] When evicting keys, the tail is checked for expiration before the normal
	//        SIEVE algorithm.

	ts = newTestSieve(3)
	ts.Set("a", 1, 20*time.Second) // A at the tail.
	advanceTime(5)
	ts.Set("b", 2, 20*time.Second) // B in the middle.
	advanceTime(5)
	ts.Set("c", 3, 20*time.Second) // C in the front.

	ts.Get("a")                    // Bump A.
	ts.Set("d", 4, 20*time.Second) // D will evict B, putting the hand in the middle. A  is skipped (visited).
	assert(t, ts.Get("b") == -1)

	advanceTime(10) // A is expired now, so A at the tail will be evicted, and not C.
	ts.Set("e", 5, 20*time.Second)
	assert(t, ts.Get("a") == -1)
	assert(t, ts.Get("c") == 3)
	assert(t, ts.Get("d") == 4)
	assert(t, ts.Get("e") == 5)
}

func TestSievesWithFixedTtl(t *testing.T) {
	useTime(0)
	defer restoreTime()

	for i := 0; i < 5; i++ {
		rnd := rand.New(rand.NewSource(int64(i)))

		ts := newTestSieve(10)
		rs := sieve.NewSieve(10)

		for j := 0; j < 10000; j++ {
			item := rnd.Intn(15)
			key := fmt.Sprintf("key%d", item)

			tsItem := ts.Get(key)
			rsItem := rs.Get(key)

			if tsItem == -1 {
				assert(t, rsItem == nil)
				ts.Set(key, item, 10*time.Second)
				rs.Set(key, item, 10*time.Second)
			} else {
				assert(t, tsItem == rsItem)
			}
			advanceTime(int64(rnd.Intn(10)))
		}
	}
}

func TestSievesWithRandomTtl(t *testing.T) {
	useTime(0)
	defer restoreTime()

	for i := 0; i < 10; i++ {
		cleaning := i >= 5

		rnd := rand.New(rand.NewSource(int64(i)))

		ts := newTestSieve(10)
		rs := sieve.NewSieve(10)

		for j := 0; j < 10000; j++ {

			item := rnd.Intn(15)
			key := fmt.Sprintf("key%d", item)

			tsItem := ts.Get(key)
			rsItem := rs.Get(key)

			if tsItem == -1 {
				assert(t, rsItem == nil)
				ttl := time.Duration(rnd.Intn(15)) * time.Second
				ts.Set(key, item, ttl)
				rs.Set(key, item, ttl)
			} else {
				assert(t, tsItem == rsItem)
			}
			advanceTime(int64(rnd.Intn(10)))
			if cleaning {
				// For iterations 5-10, do cleaning every 2 iterations
				if j%2 == 1 {
					ts.Clean()
					rs.Clean()
				}
			}
		}
	}
}

func TestNumKeys(t *testing.T) {
	useTime(0)
	defer restoreTime()

	s := sieve.NewSieve(10)

	// [SPEC] An expiration value of 1 will be rounded up to 1 second.

	s.Set("a", 1, 1)
	s.Set("b", 1, 1)
	s.Set("c", 1, 1)

	assert(t, s.NumKeys() == 3)

	s.Reset()
	assert(t, s.NumKeys() == 0)

	s.Set("a", 1, 1)
	s.Set("b", 1, 1)
	s.Set("c", 1, time.Second*4)

	advanceTime(1)

	// [SPEC] NumKeys reports the number of keys in the data structure and does not check
	//        for expiration.
	assert(t, s.NumKeys() == 3)

	s.Get("a") // this should expire and remove "a"
	assert(t, s.NumKeys() == 2)

	s.Clean()
	assert(t, s.NumKeys() == 1)

	advanceTime(4)
	s.Clean()
	assert(t, s.NumKeys() == 0)
}
