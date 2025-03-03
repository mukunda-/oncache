package oncache_test

import (
	"testing"

	"go.mukunda.com/oncache"
)

type Config string

func (s string) AsConfig() Config {
	return Config{}
}

func test1(cfg Config) {
}

func TestOncacheSingleNode(t *testing.T) {
	oc := oncache.New()
	oc.SetPort(8850)
	cache := oc.NewCache("test", oncache.NewCacheOptions{
		MaxKeys: 2,
	})

	oc.Init(make([]byte, 16))

	cache.Set("key1/key2", "value")
	cache.Set("key1/key3", "value")
	cache.Set("key1/key4", "value")

}
