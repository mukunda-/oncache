//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

// SIEVE cache implementation.
// https://junchengyang.com/publication/nsdi24-SIEVE.pdf
package sieve

import "time"

var Now = func() int64 {
	return time.Now().Unix()
}

type sieveRecord struct {
	prev     uint16
	next     uint16
	value    any
	vexpires uint32 // bit0 = Visited, bit1-31 = Expiration
}

type Sieve struct {
	// Same as len of data.
	maxKeys uint16

	// Map of all keys to data records.
	keys map[string]uint16

	// 0 is a dummy record to make addressing less error-prone. 1-maxKeys are valid
	// records. 0 is the "null" address.
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

type shortQueue struct {
	head uint16
	tail uint16
	data []sieveRecord
}

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

func (s *Sieve) pushFree(rec uint16) {
	s.data[rec].prev = 0
	s.data[rec].next = s.nextFree
	s.nextFree = rec
}

func NewSieve(maxKeys uint16) *Sieve {
	s := &Sieve{
		maxKeys: maxKeys,
	}

	s.Reset()
	return s
}

func (s *Sieve) Set(key string, value any) {
	// TODO
}

func (s *Sieve) Delete(key string) {
	entry := s.keys[key]
	if entry == 0 {
		return
	}

	delete(s.keys, key)

	data := s.data[entry]
	if data.prev != 0 {
		s.data[data.prev].next = data.next
	}
	if data.next != 0 {
		s.data[data.next].prev = data.prev
	}

	if s.head == entry {
		s.head = data.next
	}
	if s.tail == entry {
		s.tail = data.prev
	}

	s.data[entry].value = nil
	s.pushFree(entry)
}

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
