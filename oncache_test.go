package oncache_test

import (
	"testing"

	"go.mukunda.com/oncache"
)

func TestOncacheSingleNode(t *testing.T) {
	oc := oncache.New()
	oc.SetPort(8850)
	cache := oc.NewCache("test", oncache.NewCacheOptions{
		MaxKeys: 2,
	})

	oc.Init(make([]byte, 16))

	cache.Set("key1", "key1")
	cache.Set("key2", "key2")
	cache.Set("key3", "key3")

	if cache.Get("key1") != nil {
		t.Error("key1 should have been evicted")
	}

}
