package cache

import "testing"

func TestLRUEvictionPolicy_EvictionOrder(t *testing.T) {
	l := NewLRUEvictionPolicy()

	l.Add("A", 0)
	l.Add("B", 0)
	l.Add("C", 0)

	l.Touch("A")

	// First eviction
	e, _, ok := l.Evict()

	if !ok {
		t.Error("Evict function returned ok=false")
	}

	if e != "B" {
		t.Errorf("Evicted key %s doesn't match expected %s", e, "B")
	}

	// Second eviction
	e, _, ok = l.Evict()

	if !ok {
		t.Error("Evict function returned ok=false")
	}

	if e != "C" {
		t.Errorf("Evicted key %s doesn't match expected %s", e, "B")
	}
}

func TestLRUEvictionPolicy_SizeTracking(t *testing.T) {
	l := NewLRUEvictionPolicy()

	l.Add("A", 10)
	l.Add("B", 10)
	l.Add("C", 10)

	cb := l.CurrentBytes()

	if cb != 30 {
		t.Errorf("Size %d doesn't match the expected %d", cb, 30)
	}
}

func TestLRUEvictionPolicy_EmptyEviction(t *testing.T) {
	l := NewLRUEvictionPolicy()

	_, _, ok := l.Evict()

	if ok {
		t.Error("Expected ok=false, got ok=true")
	}
}
