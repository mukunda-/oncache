//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package mapchain

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestFilterJob(t *testing.T) {
	mc := NewMapChain()

	badKeys := 0
	goodKeys := 0
	keys := []string{}

	newKeyValue := func() any {
		if rand.Intn(2) == 0 {
			goodKeys++
			return "good"
		} else {
			badKeys++
			return "bad"
		}
	}

	countAll := func() (int, int) {
		bad := 0
		good := 0
		for _, key := range keys {
			val := mc.Get(key)
			if val == "good" {
				good++
			} else if val == "bad" {
				bad++
			}
		}
		return good, bad
	}

	// Create a tree of keys, random number of entries per key, 3 levels of max depth.
	for i := 0; i < 20; i++ {
		key1 := fmt.Sprintf("base%d", i)

		subkeys := rand.Intn(20)
		if subkeys == 0 {
			// If only one key, this is a value, not a node.
			mc.Set(key1, newKeyValue())
			keys = append(keys, key1)
		} else {
			for j := 0; j < subkeys; j++ {
				key2 := fmt.Sprintf("%s/alpha%d", key1, j)

				// Up to 10 more sub keys
				subkeys := rand.Intn(10)
				if subkeys == 0 {
					// If only one key, this is a value, not a node.
					mc.Set(key2, newKeyValue())
					keys = append(keys, key2)
				} else {
					// Final set of keys. depth is 3.
					subkeys := rand.Intn(4) + 1
					for k := 0; k < subkeys; k++ {
						key3 := fmt.Sprintf("%s/bravo%d", key2, k)
						mc.Set(key3, newKeyValue())
						keys = append(keys, key3)
					}
				}
			}
		}
	}

	{
		good, bad := countAll()
		assertEq(t, good, goodKeys)
		assertEq(t, bad, badKeys)

	}

	filter := NewFilterJob(mc, func(value any) bool {
		return value == "good"
	})
	filter.BatchSize = 100

	goodProcessed := 0
	badProcessed := 0
	totalProcessed := 0
	iterations := 0

	for badProcessed < badKeys {
		iterations++
		processed, removed, done := filter.Run()
		totalProcessed += processed
		if !done {
			assertEq(t, processed, filter.BatchSize)
		}
		goodProcessed += processed - removed
		badProcessed += removed
		t.Log("Processed", processed, "removed", removed, "done", done, "goodproc", goodProcessed, "badproc", badProcessed)
		if badKeys < 0 {
			t.Error("Removed more bad keys than expected.")
			t.FailNow()
		}
		assertEq(t, done, badProcessed == badKeys)

		good, bad := countAll()
		assertEq(t, good, goodKeys)
		assertEq(t, bad, badKeys-badProcessed)
	}

	{
		assertEq(t, totalProcessed, goodKeys+badKeys)
		good, bad := countAll()
		assertEq(t, goodProcessed, good)
		assertEq(t, bad, 0)
		assertEq(t, iterations, (goodKeys+badKeys+filter.BatchSize-1)/filter.BatchSize)
	}

	assertEq(t, goodProcessed, goodKeys)
}
