//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package mapchain

import (
	"testing"
)

func assertEq(t *testing.T, value, expected any) {
	t.Helper()
	if value != expected {
		t.Error(value, "does not equal", expected)
	}
}

func TestMapChain(t *testing.T) {
	mc := Mapchain{make(Node)}

	// [SPEC] Basic key-value setting and getting works the same way as a normal map.
	mc.Set("a/b/c", 123)
	mc.Set("a/b/d", 456)

	assertEq(t, mc.Get("a/b/c"), 123)
	assertEq(t, mc.Get("a/b/d"), 456)

	// [SPEC] Internally, the map is divided into nodes, separated by each slash in the
	//        key. Reading the address of a "node" will return nil.
	assertEq(t, mc.Get("a/b"), nil)
	assertEq(t, mc.Get("a"), nil)
	assertEq(t, mc.Get(""), nil)

	// [SPEC] Setting values of a path overwrites any sub-nodes.
	mc.Set("a/b", 555)
	assertEq(t, mc.Get("a/b"), 555)
	assertEq(t, mc.Get("a/b/c"), nil) // Sub-key should be erased by parent key.

	// [SPEC] Deleting a key also deletes all sub-keys attached to it.
	mc.Set("a", nil)
	assertEq(t, mc.Get("a/b"), nil) // Sub-key should be deleted.
}

func TestEmptyKeys(t *testing.T) {
	// [SPEC] Empty "" keys are valid keys.

	mc := Mapchain{make(Node)}

	mc.Set("", 123)
	assertEq(t, mc.Get(""), 123)

	mc.Set("/", 234) // This is "" -> ""
	assertEq(t, mc.Get("/"), 234)

	// The first value is overwritten by the second.
	assertEq(t, mc.Get(""), nil)
}
