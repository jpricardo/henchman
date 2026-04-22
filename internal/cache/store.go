package cache

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type Store struct {
	mu       sync.RWMutex
	data     map[string]*Entry
	policy   EvictionPolicy
	maxBytes int64
	maxKeys  int64
}

func NewStore(policy EvictionPolicy, maxBytes int64, maxKeys int64) *Store {
	return &Store{
		mu:       sync.RWMutex{},
		data:     map[string]*Entry{},
		policy:   policy,
		maxBytes: maxBytes,
		maxKeys:  maxKeys,
	}
}

func (s *Store) Set(entry *Entry) error {
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

func (s *Store) Get(key string, invalidateMatched bool) (*Entry, bool) {
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

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.remove(key)
}

func (s *Store) remove(key string) {
	delete(s.data, key)
	s.policy.Remove(key)
}

func (s *Store) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string]*Entry)
	s.policy.Reset()
}

func (s *Store) Sweep() int64 {
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

func (s *Store) Query(keyPrefix string, invalidateMatched bool) []*Entry {
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
