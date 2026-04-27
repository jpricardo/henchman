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
		t.Errorf("Evicted key %s doesn't match expected %s", e, "C")
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

func TestLFUEvictionPolicy_EvictionOrder(t *testing.T) {
	l := NewLFUEvictionPolicy()

	l.Add("A", 0)
	l.Add("B", 0)
	l.Add("C", 0)

	l.Touch("A")
	l.Touch("A")
	l.Touch("B")

	e, _, ok := l.Evict()
	if !ok {
		t.Error("Evict function returned ok=false")
	}
	if e != "C" {
		t.Errorf("Evicted key %s doesn't match expected %s", e, "C")
	}

	e, _, ok = l.Evict()
	if !ok {
		t.Error("Evict function returned ok=false")
	}
	if e != "B" {
		t.Errorf("Evicted key %s doesn't match expected %s", e, "B")
	}

	e, _, ok = l.Evict()
	if !ok {
		t.Error("Evict function returned ok=false")
	}
	if e != "A" {
		t.Errorf("Evicted key %s doesn't match expected %s", e, "A")
	}
}

func TestLFUEvictionPolicy_LRUTiebreak(t *testing.T) {
	l := NewLFUEvictionPolicy()

	l.Add("A", 0)
	l.Add("B", 0)
	l.Add("C", 0)

	e, _, ok := l.Evict()
	if !ok {
		t.Error("Evict function returned ok=false")
	}
	if e != "A" {
		t.Errorf("Evicted key %s doesn't match expected %s (LRU among equal-frequency entries)", e, "A")
	}
}

func TestLFUEvictionPolicy_SizeTracking(t *testing.T) {
	l := NewLFUEvictionPolicy()

	l.Add("A", 10)
	l.Add("B", 10)
	l.Add("C", 10)

	if cb := l.CurrentBytes(); cb != 30 {
		t.Errorf("Size %d doesn't match the expected %d", cb, 30)
	}
	if ck := l.CurrentKeys(); ck != 3 {
		t.Errorf("Keys %d doesn't match the expected %d", ck, 3)
	}

	l.Touch("A")
	l.Touch("A")

	if cb := l.CurrentBytes(); cb != 30 {
		t.Errorf("Size after Touch %d doesn't match the expected %d", cb, 30)
	}

	l.Remove("B")

	if cb := l.CurrentBytes(); cb != 20 {
		t.Errorf("Size after Remove %d doesn't match the expected %d", cb, 20)
	}
	if ck := l.CurrentKeys(); ck != 2 {
		t.Errorf("Keys after Remove %d doesn't match the expected %d", ck, 2)
	}
}

func TestLFUEvictionPolicy_EmptyEviction(t *testing.T) {
	l := NewLFUEvictionPolicy()

	_, _, ok := l.Evict()

	if ok {
		t.Error("Expected ok=false, got ok=true")
	}
}

func TestLFUEvictionPolicy_ReaddResetsFrequency(t *testing.T) {
	l := NewLFUEvictionPolicy()

	l.Add("A", 0)
	l.Touch("A")
	l.Touch("A")
	l.Touch("A")

	l.Add("A", 0)
	l.Add("B", 0)
	l.Touch("B")

	e, _, ok := l.Evict()
	if !ok {
		t.Error("Evict function returned ok=false")
	}
	if e != "A" {
		t.Errorf("Evicted key %s doesn't match expected %s (re-added A should reset to freq 1)", e, "A")
	}
}
