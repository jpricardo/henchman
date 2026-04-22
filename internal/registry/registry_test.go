package registry

import (
	"math"
	"testing"
	"time"

	"github.com/jpricardo/henchman/internal/budget"
	"github.com/jpricardo/henchman/internal/cache"
	"go.uber.org/goleak"
)

func TestRegistry_Registration(t *testing.T) {
	gb := budget.NewGlobalBudget(math.MaxInt64)
	r := NewRegistry(gb)
	c := RegisterConfig{
		policy:   cache.NewLRUEvictionPolicy(),
		maxBytes: 512,
		maxKeys:  20,
	}

	token, err := r.Register("test-instance", c)
	if err != nil {
		t.Error(err.Error())
	}
	defer r.Flush(token)

	if i, ok := r.Resolve(token); i == nil || !ok {
		t.Error("Expected Resolve to return an instance")
	}
}

func TestRegistry_TokenUniqueness(t *testing.T) {
	gb := budget.NewGlobalBudget(math.MaxInt64)
	r := NewRegistry(gb)
	c := RegisterConfig{
		policy:   cache.NewLRUEvictionPolicy(),
		maxBytes: 512,
		maxKeys:  20,
	}

	t1, err := r.Register("test-instance-1", c)
	if err != nil {
		t.Error(err.Error())
	}
	defer r.Flush(t1)

	t2, err := r.Register("test-instance-2", c)
	if err != nil {
		t.Error(err.Error())
	}
	defer r.Flush(t2)

	if t1 == t2 {
		t.Error("Expected tokens to be unique")
	}
}

func TestRegistry_ReRegistration(t *testing.T) {
	gb := budget.NewGlobalBudget(math.MaxInt64)
	r := NewRegistry(gb)
	c := RegisterConfig{
		policy:   cache.NewLRUEvictionPolicy(),
		maxBytes: 512,
		maxKeys:  20,
	}

	t1, err := r.Register("test-instance-1", c)
	if err != nil {
		t.Error(err.Error())
	}
	defer r.Flush(t1)

	i1 := r.byToken[t1]
	e := cache.Entry{Key: "test-key"}

	if err := i1.store.Set(&e); err != nil {
		t.Error(err.Error())
	}

	t2, err := r.Register("test-instance-1", c)
	if err != nil {
		t.Error(err.Error())
	}
	defer r.Flush(t2)

	if _, ok := i1.store.Get(e.Key); ok {
		t.Error("Expected Get to fail")
	}
}

func TestRegistry_BudgetRelease(t *testing.T) {
	gb := budget.NewGlobalBudget(512)
	r := NewRegistry(gb)
	c := RegisterConfig{
		policy:   cache.NewLRUEvictionPolicy(),
		maxBytes: 512,
		maxKeys:  20,
	}

	t1, err := r.Register("test-instance-1", c)
	if err != nil {
		t.Error(err.Error())
	}
	defer r.Flush(t1)

	t2, err := r.Register("test-instance-2", c)
	if err == nil {
		t.Error("Expected Register to fail")
	}
	defer r.Flush(t2)

	r.Flush(t1)

	t3, err := r.Register("test-instance-2", c)
	if err != nil {
		t.Error(err.Error())
	}
	defer r.Flush(t3)
}

func TestRegistry_Sweep(t *testing.T) {
	gb := budget.NewGlobalBudget(math.MaxInt64)
	r := NewRegistry(gb)
	c := RegisterConfig{
		policy:        cache.NewLRUEvictionPolicy(),
		maxBytes:      512,
		maxKeys:       20,
		sweepInterval: 50 * time.Millisecond,
	}

	token, err := r.Register("test-instance-1", c)
	if err != nil {
		t.Error(err.Error())
	}
	defer r.Flush(token)

	i1 := r.byToken[token]

	e := cache.Entry{Key: "test-key", ExpiresAt: time.Now()}
	if err := i1.store.Set(&e); err != nil {
		t.Error(err.Error())
	}

	time.Sleep(2 * c.sweepInterval)

	if i, ok := i1.store.Get("test-key"); i != nil || ok {
		t.Error("Expected key to be gone")
	}

	if i1.metrics.ExpiredKeys() == 0 {
		t.Errorf("Expected expired keys to equal 1, got %d", i1.metrics.expiredKeys)
	}
}

func TestRegistry_SweepStopsOnContextCancel(t *testing.T) {
	gb := budget.NewGlobalBudget(math.MaxInt64)
	r := NewRegistry(gb)
	c := RegisterConfig{
		policy:        cache.NewLRUEvictionPolicy(),
		maxBytes:      512,
		maxKeys:       20,
		sweepInterval: 50 * time.Millisecond,
	}

	token, _ := r.Register("test-instance", c)
	r.Flush(token)

	goleak.VerifyNone(t)
}
