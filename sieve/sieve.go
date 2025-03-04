//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

/*
This package provides a SIEVE cache implementation.
https://junchengyang.com/publication/nsdi24-SIEVE.pdf

This implementation includes a time-to-live (TTL) option for values.

One implementation specific detail is how the cache handles setting a key multiple times.
When updating an existing key, the "visited" flag is not set. If the value has a TTL, then
the value is moved to the head.
*/
package sieve

import "time"

// For [sieveRecord.vexpires].
const kVeVisited = 1
const kVeExpires = 0xFFFFFFFE

// For debug purposes. Should always be 0 unless there is a bug.
var ErrFlag uint32 = 0

// This returns the current unixtime. Can be overwritten by tests.
var Now = func() int64 {
	return time.Now().Unix()
}

// All records are allocated upfront
type sieveRecord struct {
	key      string
	prev     uint16 // 0 = null
	next     uint16 // 0 = null
	value    any
	vexpires uint32 // bit0 = Visited, bit1-31 = Expiration
}

type Sieve struct {
	// Same as len of data.
	maxKeys uint16

	// Map of all keys to data records.
	keys map[string]uint16

	// 0 is a dummy record to make addressing easier. 1-maxKeys are valid records. 0 is the
	// "null" address.
	data []sieveRecord

	// Linked list of active records.
	head uint16
	tail uint16
	hand uint16

	// Offset for expiration times.
	epoch int64

	// The Free list is singly-linked.
	nextFree uint16
}

// Remove a record from the free list and return its ID.
func (s *Sieve) popFree() uint16 {
	if s.nextFree == 0 {
		return 0
	}

	rec := s.nextFree
	s.nextFree = s.data[rec].next

	s.data[rec].prev = 0
	s.data[rec].next = 0
	return rec
}

// Add a record by ID to the free list.
func (s *Sieve) pushFree(rec uint16) {
	s.data[rec].prev = 0
	s.data[rec].next = s.nextFree
	s.nextFree = rec
}

// Remove a record by ID from the active set. Does not clear `value` in case we want to
// readd it.
func (s *Sieve) pop(rec uint16) {
	// If we are the hand, progress it.
	datarec := &s.data[rec]

	if s.hand == rec {
		s.hand = datarec.prev
	}

	if datarec.prev != 0 {
		s.data[datarec.prev].next = datarec.next
	} else {
		s.head = datarec.next
	}

	if datarec.next != 0 {
		s.data[datarec.next].prev = datarec.prev
	} else {
		s.tail = datarec.prev
	}

	datarec.prev = 0
	datarec.next = 0
}

// Add a record by ID to the active set.
func (s *Sieve) push(rec uint16) {
	if s.head != 0 {
		s.data[s.head].prev = rec
	}

	s.data[rec].prev = 0
	s.data[rec].next = s.head
	s.head = rec

	if s.tail == 0 {
		s.tail = rec
	}
}

// Remove a record from the active set using the SIEVE algorithm and return its ID.
// We also check the tail first to see if the ttl is expired. If all keys use the same
// expiration, then they will be in expiration order.
func (s *Sieve) evict() uint16 {
	{
		rec := s.tail
		if rec == 0 {
			// sanity check - should not be reachable
			ErrFlag |= 1
			return 0
		}
		datarec := &s.data[rec]
		exp := datarec.vexpires >> 1
		if exp != 0 && Now() >= s.epoch+int64(exp) {
			if s.hand == rec {
				s.hand = datarec.prev
			}
			s.pop(rec)
			datarec.value = nil
			delete(s.keys, datarec.key)
			return rec
		}
	}

	if s.hand == 0 {
		// Start a new round.
		s.hand = s.tail
	}

	rec := s.hand
	now := Now()
	for {
		datarec := &s.data[rec]
		exp := datarec.vexpires >> 1
		if exp != 0 && now >= s.epoch+int64(exp) {
			// This record is expired, use it.
		} else if datarec.vexpires&kVeVisited == 0 {
			// This record has not been visited. use it.
		} else {
			datarec.vexpires &= kVeExpires
			rec = datarec.prev
			if rec == 0 {
				// Start a new round.
				rec = s.tail
			}
			continue
		}
		s.hand = datarec.prev
		s.pop(rec)
		datarec.value = nil
		delete(s.keys, datarec.key)
		return rec
	}
}

// Create a new SIEVE cache with the given max amount of keys.
func NewSieve(maxKeys uint16) *Sieve {
	s := &Sieve{
		maxKeys: maxKeys,
	}

	s.Reset()
	return s
}

// Returns a key from the cache. If it is not found, returns nil.
func (s *Sieve) Get(key string) any {
	rec := s.keys[key]
	if rec == 0 {
		return nil
	}

	datarec := &s.data[rec]
	exp := datarec.vexpires >> 1
	if exp != 0 && Now() >= s.epoch+int64(exp) {
		// Expired.
		if s.hand == rec {
			s.hand = datarec.prev
		}
		s.pop(rec)
		s.pushFree(rec)
		datarec.value = nil
		delete(s.keys, datarec.key)
		return nil
	}

	// Set the visited flag.
	datarec.vexpires |= kVeVisited
	return datarec.value
}

func (s *Sieve) translateExpiresValue(ttl time.Duration) uint32 {
	if ttl == 0 {
		return 0
	}
	return uint32((Now() - s.epoch + int64(ttl.Seconds())) << 1)
}

// Update a value in the cache by key. `ttl` is an expiration time in seconds. 0 = no
// expiration.
func (s *Sieve) Set(key string, value any, ttl time.Duration) {
	rec := s.keys[key]
	if rec != 0 {

		// Entry exists in the cache. Update the value. If ttl is set, then bump it to the
		// top. Otherwise, leave it where it is. These operations are not defined in the
		// paper (overwriting or ttl behavior).
		s.data[rec].value = value
		s.data[rec].vexpires = s.data[rec].vexpires&kVeVisited |
			s.translateExpiresValue(ttl)

		if ttl != 0 {
			// Move to front if TTL is present.
			s.pop(rec)
			s.push(rec)
		} else {
			// It may not be beneficial to mark as visited if it's not being read.
			//s.data[rec].vexpires |= kVeVisited
		}
		return
	}

	rec = s.popFree()
	if rec == 0 {
		rec = s.evict()
		if rec == 0 {
			// sanity check - should not be reachable
			ErrFlag |= 2
			return
		}
	}

	s.keys[key] = rec
	s.data[rec].key = key
	s.data[rec].value = value
	s.data[rec].vexpires = s.translateExpiresValue(ttl)
	s.push(rec)
}

// Invalidate a value from the cache by key. Does nothing if the key doesn't exist.
func (s *Sieve) Delete(key string) {
	rec := s.keys[key]
	if rec == 0 {
		return
	}
	delete(s.keys, key)
	s.pop(rec)
	s.data[rec].value = nil
	s.pushFree(rec)
}

// Release all keys and reset the cache to an empty state.
func (s *Sieve) Reset() {
	s.head = 0
	s.tail = 0
	s.hand = 0
	s.nextFree = 0
	s.epoch = Now()
	s.keys = make(map[string]uint16)
	s.data = make([]sieveRecord, s.maxKeys+1)

	for i := uint16(1); i <= s.maxKeys; i++ {
		s.pushFree(i)
	}
}

// Delete all expired keys.
func (s *Sieve) Clean() {
	for rec := s.head; rec != 0; {
		next := s.data[rec].next
		exp := s.data[rec].vexpires >> 1
		if exp != 0 && Now() >= s.epoch+int64(exp) {
			s.pop(rec)
			s.data[rec].value = nil
			delete(s.keys, s.data[rec].key)
			s.pushFree(rec)
		}
		rec = next
	}
}
