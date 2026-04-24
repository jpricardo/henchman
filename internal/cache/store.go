package cache

import (
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"
)

// #region StoreShard
type StoreShard struct {
	mu       sync.RWMutex
	data     map[string]*Entry
	policy   EvictionPolicy
	maxBytes int64
	maxKeys  int64
}

func NewStoreShard(policy EvictionPolicy, maxBytes int64, maxKeys int64) *StoreShard {

	return &StoreShard{
		mu:       sync.RWMutex{},
		data:     map[string]*Entry{},
		policy:   policy,
		maxBytes: maxBytes,
		maxKeys:  maxKeys,
	}
}

func (s *StoreShard) Set(entry *Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	es := entry.SizeBytes()
	if es > s.maxBytes {
		return fmt.Errorf("entry size %d exceeds maximum size %d", es, s.maxBytes)
	}

	// Make room
	for {
		newSize := s.policy.CurrentBytes() + es
		newKeys := s.policy.CurrentKeys() + 1

		if newSize <= s.maxBytes && newKeys <= s.maxKeys {
			break
		}

		ev, _, ok := s.policy.Evict()
		if !ok {
			break
		}

		delete(s.data, ev)
	}

	s.data[entry.Key] = entry
	s.policy.Add(entry.Key, entry.SizeBytes())

	return nil
}

func (s *StoreShard) Get(key string, invalidateMatched bool) (*Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.data[key]
	if e == nil || (!e.ExpiresAt.IsZero() && e.ExpiresAt.Before(time.Now())) {
		return nil, false
	}

	if e.InvalidateAfterRead || invalidateMatched {
		s.remove(e.Key)
	} else {
		s.policy.Touch(e.Key)
	}

	return e, true
}

func (s *StoreShard) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.remove(key)
}

func (s *StoreShard) remove(key string) {
	delete(s.data, key)
	s.policy.Remove(key)
}

func (s *StoreShard) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string]*Entry)
	s.policy.Reset()
}

func (s *StoreShard) Sweep() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := int64(0)

	for k := range s.data {
		v, ok := s.data[k]
		if !ok {
			continue
		}

		if !v.ExpiresAt.IsZero() && v.ExpiresAt.Before(time.Now()) {
			s.remove(v.Key)
			e++
		}
	}

	return e
}

func (s *StoreShard) Evictions() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.policy.Evictions()
}

func (s *StoreShard) CurrentBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.policy.CurrentBytes()
}

func (s *StoreShard) CurrentKeys() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.policy.CurrentKeys()
}

func (s *StoreShard) Query(keyPrefix string, invalidateMatched bool) []*Entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := []*Entry{}

	for k := range s.data {
		if !strings.HasPrefix(k, keyPrefix) {
			continue
		}

		v, ok := s.data[k]
		if !ok {
			continue
		}

		if !v.ExpiresAt.IsZero() && v.ExpiresAt.Before(time.Now()) {
			continue
		}

		r = append(r, v)

		if invalidateMatched {
			s.remove(k)
		}
	}

	return r
}

// #endregion

// #region SharedStore
type SharedStore struct {
	shards []*StoreShard
}

func NewSharedStore(shardCount int64, policyFactory func() EvictionPolicy, maxBytes int64, maxKeys int64) *SharedStore {
	shards := []*StoreShard{}

	for range shardCount {
		shards = append(shards, NewStoreShard(policyFactory(), maxBytes/shardCount, maxKeys/shardCount))
	}

	return &SharedStore{
		shards: shards,
	}
}

// #endregion

func (s *SharedStore) Set(entry *Entry) error {
	idx := shardIndex(entry.Key, len(s.shards))
	shard := s.shards[idx]

	return shard.Set(entry)
}

func (s *SharedStore) Get(key string, invalidateMatched bool) (*Entry, bool) {
	idx := shardIndex(key, len(s.shards))
	shard := s.shards[idx]

	e, found := shard.Get(key, invalidateMatched)
	if !found {
		return nil, found
	}

	return e, true
}

func (s *SharedStore) Delete(key string) {
	idx := shardIndex(key, len(s.shards))
	shard := s.shards[idx]

	shard.Delete(key)
}

func (s *SharedStore) Flush() {
	ch := make(chan struct{})
	defer close(ch)

	for _, shard := range s.shards {
		go func(ch chan<- struct{}) {
			shard.Flush()
			ch <- struct{}{}
		}(ch)
	}

	for range s.shards {
		<-ch
	}
}

func (s *SharedStore) Sweep() int64 {
	e := int64(0)

	ch := make(chan int64)
	defer close(ch)

	for _, shard := range s.shards {
		go func(ch chan<- int64) { ch <- shard.Sweep() }(ch)
	}

	for range s.shards {
		e += <-ch
	}

	return e
}

func (s *SharedStore) Evictions() int64 {
	e := int64(0)

	ch := make(chan int64)
	defer close(ch)

	for _, shard := range s.shards {
		go func(ch chan<- int64) { ch <- shard.Evictions() }(ch)
	}

	for range s.shards {
		e += <-ch
	}

	return e
}

func (s *SharedStore) MaxBytes() int64 {
	t := int64(0)

	ch := make(chan int64)
	defer close(ch)

	for _, shard := range s.shards {
		go func(ch chan<- int64) {
			ch <- shard.maxBytes
		}(ch)
	}

	for range s.shards {
		t += <-ch
	}

	return t
}

func (s *SharedStore) CurrentBytes() int64 {
	t := int64(0)

	ch := make(chan int64)
	defer close(ch)

	for _, shard := range s.shards {
		go func(ch chan<- int64) { ch <- shard.CurrentBytes() }(ch)
	}

	for range s.shards {
		t += <-ch
	}

	return t
}

func (s *SharedStore) MaxKeys() int64 {
	t := int64(0)

	ch := make(chan int64)
	defer close(ch)

	for _, shard := range s.shards {
		go func(ch chan<- int64) { ch <- shard.maxKeys }(ch)
	}

	for range s.shards {
		t += <-ch
	}

	return t
}

func (s *SharedStore) CurrentKeys() int64 {
	t := int64(0)

	ch := make(chan int64)
	defer close(ch)

	for _, shard := range s.shards {
		go func(ch chan<- int64) { ch <- shard.CurrentKeys() }(ch)
	}

	for range s.shards {
		t += <-ch
	}

	return t
}

func (s *SharedStore) Query(keyPrefix string, invalidateMatched bool) []*Entry {
	r := []*Entry{}

	ch := make(chan []*Entry)
	defer close(ch)

	for _, shard := range s.shards {
		go func(ch chan<- []*Entry) {
			ch <- shard.Query(keyPrefix, invalidateMatched)
		}(ch)
	}

	for range s.shards {
		matches := <-ch

		for _, match := range matches {
			r = append(r, match)
		}
	}

	return r
}

func shardIndex(key string, numShards int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) & (numShards - 1)
}
