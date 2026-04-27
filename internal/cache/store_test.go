package cache

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestStore_CRUD(t *testing.T) {
	s, err := NewSharedStore(16, func() EvictionPolicy { return NewLRUEvictionPolicy() }, math.MaxInt64, 16)
	if err != nil {
		t.Fatal(err)
	}

	e := Entry{Key: "key"}

	err = s.Set(&e)
	if err != nil {
		t.Error(err.Error())
	}

	if _, ok := s.Get(e.Key, false); !ok {
		t.Errorf("Unable to get key %s, got ok=false", e.Key)
	}

	s.Delete(e.Key)

	if _, ok := s.Get(e.Key, false); ok {
		t.Errorf("Shouldn't have been able to get deleted key %s, got ok=true", e.Key)
	}
}

func TestStore_EvictionOnFull(t *testing.T) {
	// don't bother testing `maxBytes`, any change on the `Entry` struct could make the tests fail
	s, err := NewSharedStore(1, func() EvictionPolicy { return NewLRUEvictionPolicy() }, math.MaxInt64, 5)
	if err != nil {
		t.Fatal(err)
	}

	for idx := range s.MaxKeys() {
		err := s.Set(&Entry{Key: fmt.Sprintf("key-%d", idx+1)})
		if err != nil {
			t.Error(err.Error())
		}
	}

	if s.CurrentKeys() != s.MaxKeys() {
		t.Errorf("Expected store to be full, got %d keys", s.CurrentKeys())
	}

	e := Entry{Key: "last-key"}
	err = s.Set(&e)
	if err != nil {
		t.Error(err.Error())
	}

	if _, ok := s.Get(e.Key, false); !ok {
		t.Errorf("Expected key %s to have been added to the store", e.Key)
	}

	if _, ok := s.Get("key-1", false); ok {
		t.Error("Expected key-1 to have been evicted")
	}

	if s.MaxKeys() != s.CurrentKeys() {
		t.Errorf("Expected current keys %d to match max keys %d", s.CurrentKeys(), s.MaxKeys())
	}
}

func TestStore_OversizedEntry(t *testing.T) {
	// shardCount=1: testing per-entry size rejection, not sharding behaviour.
	s, err := NewSharedStore(1, func() EvictionPolicy { return NewLRUEvictionPolicy() }, 1, 1)
	if err != nil {
		t.Fatal(err)
	}

	e := Entry{}
	err = s.Set(&e)

	if err == nil {
		t.Errorf("Expected insertion to fail on max bytes %d", s.MaxBytes())
	}
}

func TestStore_RejectsMaxKeysBelowShardCount(t *testing.T) {
	_, err := NewSharedStore(16, func() EvictionPolicy { return NewLRUEvictionPolicy() }, math.MaxInt64, 5)
	if err == nil {
		t.Fatal("expected NewSharedStore to reject maxKeys < shardCount, got nil error")
	}
}

func TestStore_RejectsMaxBytesBelowShardCount(t *testing.T) {
	_, err := NewSharedStore(16, func() EvictionPolicy { return NewLRUEvictionPolicy() }, 8, math.MaxInt64)
	if err == nil {
		t.Fatal("expected NewSharedStore to reject maxBytes < shardCount, got nil error")
	}
}

func TestStore_RefusesWriteWhenEvictionCannotFreeRoom(t *testing.T) {
	s := NewStoreShard(NewLRUEvictionPolicy(), math.MaxInt64, 0)

	err := s.Set(&Entry{Key: "k"})
	if err == nil {
		t.Error("expected Set to fail when eviction cannot free room, got nil")
	}

	if s.CurrentKeys() != 0 {
		t.Errorf("expected store to remain empty after failed Set, got %d keys", s.CurrentKeys())
	}
}

func TestStore_ExpiredKey(t *testing.T) {
	s, err := NewSharedStore(16, func() EvictionPolicy { return NewLRUEvictionPolicy() }, math.MaxInt64, 16)
	if err != nil {
		t.Fatal(err)
	}

	e := Entry{ExpiresAt: time.Now().Add(-10 * time.Minute)}
	err = s.Set(&e)
	if err != nil {
		t.Error(err.Error())
	}

	if _, ok := s.Get(e.Key, false); ok {
		t.Error("Expected Get to fail on expired key")
	}
}
