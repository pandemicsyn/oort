package mapstore

import (
	"sync"
)

type MapCache struct {
	sync.RWMutex
	data map[string][]byte
}

func NewMapCache() *MapCache {
	mapcache := MapCache{
		data: make(map[string][]byte),
	}

	return &mapcache
}

func (cache *MapCache) Get(key []byte, value []byte) []byte {
	cache.RLock()
	value = append(value, cache.data[string(key)]...)
	cache.RUnlock()
	return value
}

func (cache *MapCache) Set(key []byte, value []byte) {
	cache.Lock()
	val := make([]byte, len(value))
	copy(val, value)
	cache.data[string(key)] = val
	cache.Unlock()
}
