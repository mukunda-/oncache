//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

// This package provides an implementation for a map that supports nested keys.
package mapchain

import "strings"

type Node map[string]any

// Mapchain is a map that supports nested keys. The full key is formatted as
// "aaa/bbb/ccc", with slash as a separator. The value can be anything but nil. Nil
// indicates a lack of value.
type Mapchain struct {
	base Node
}

func NewMapChain() *Mapchain {
	return &Mapchain{make(Node)}
}

// Get with the ability to include nodes.
func (mc *Mapchain) getEx(key string, includeNodes bool) (value any) {
	keys := strings.Split(key, "/")
	m := mc.base
	for _, part := range keys[:len(keys)-1] {
		next, ok := m[part].(Node)
		if !ok {
			return nil
		}
		m = next
	}

	value = m[keys[len(keys)-1]]
	if _, ok := value.(Node); ok && !includeNodes {
		// Do not return nodes.
		return nil
	}

	return value
}

// Get a value from the mapchain. The key is a slash-separated chain of keys, e.g.,
// "aaa/bbb/ccc". Returns nil if the value doesn't exist.
func (mc *Mapchain) Get(key string) any {
	return mc.getEx(key, false)
}

// Set a value in the mapchain. The key is a slash-separated path. If value is nil, the
// key is deleted.
func (mc *Mapchain) Set(key string, value any) {
	keys := strings.Split(key, "/")

	nodes := make([]Node, 0, 10)
	m := mc.base
	nodes = append(nodes, m)

	for _, part := range keys[:len(keys)-1] {

		mnext, ok := m[part].(Node)
		if !ok {
			mnext = make(Node)
			m[part] = mnext
		}

		nodes = append(nodes, mnext)
		m = mnext
	}

	if value == nil {
		delete(m, keys[len(keys)-1])

		for i := len(nodes) - 1; i >= 1; i-- {
			if len(nodes[i]) == 0 {
				delete(nodes[i-1], keys[i])
			}
		}
	} else {
		// Note this will happily overwrite complete nodes.
		m[keys[len(keys)-1]] = value
	}
}
