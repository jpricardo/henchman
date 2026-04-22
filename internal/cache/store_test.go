package cache

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

func TestStore_CRUD(t *testing.T) {
	s := Store{
		mu:       sync.RWMutex{},
		data:     make(map[string]*Entry),
		policy:   NewLRUEvictionPolicy(),
		maxBytes: math.MaxInt64,
		maxKeys:  5,
	}

	e := Entry{Key: "key"}

	err := s.Set(&e)
	if err != nil {
		t.Error(err.Error())
	}

	if _, ok := s.Get(e.Key); !ok {
		t.Errorf("Unable to get key %s, got ok=false", e.Key)
	}

	s.Delete(e.Key)

	if _, ok := s.Get(e.Key); ok {
		t.Errorf("Shouldn't have been able to get deleted key %s, got ok=true", e.Key)
	}
}

func TestStore_EvictionOnFull(t *testing.T) {
	// don't bother testing `maxBytes`, any change on the `Entry` struct could make the tests fail
	s := NewStore(NewLRUEvictionPolicy(), math.MaxInt64, 5)

	for idx := range s.maxKeys {
		err := s.Set(&Entry{Key: fmt.Sprintf("key-%d", idx+1)})
		if err != nil {
			t.Error(err.Error())
		}
	}

	if s.policy.CurrentKeys() != s.maxKeys {
		t.Errorf("Expected store to be full, got %d keys", s.policy.CurrentKeys())
	}

	e := Entry{Key: "last-key"}
	err := s.Set(&e)
	if err != nil {
		t.Error(err.Error())
	}

	if _, ok := s.Get(e.Key); !ok {
		t.Errorf("Expected key %s to have been added to the store", e.Key)
	}

	if _, ok := s.Get("key-1"); ok {
		t.Error("Expected key-1 to have been evicted")
	}

	if s.maxKeys != s.policy.CurrentKeys() {
		t.Errorf("Expected current keys %d to match max keys %d", s.policy.CurrentKeys(), s.maxKeys)
	}
}

func TestStore_OversizedEntry(t *testing.T) {
	s := NewStore(NewLRUEvictionPolicy(), 1, 1)

	e := Entry{}
	err := s.Set(&e)

	if err == nil {
		t.Errorf("Expected insertion to fail on max bytes %d", s.maxBytes)
	}
}

func TestStore_ExpiredKey(t *testing.T) {
	s := NewStore(NewLRUEvictionPolicy(), math.MaxInt64, 5)

	e := Entry{ExpiresAt: time.Now().Add(-10 * time.Minute)}
	err := s.Set(&e)
	if err != nil {
		t.Error(err.Error())
	}

	if _, ok := s.Get(e.Key); ok {
		t.Error("Expected Get to fail on expired key")
	}
}
