package registry

import (
	"math"
	"testing"

	"github.com/jpricardo/henchman/internal/budget"
	"github.com/jpricardo/henchman/internal/cache"
)

func TestRegistry_Registration(t *testing.T) {
	gb := budget.NewGlobalBudget(math.MaxInt64)
	r := NewRegistry(gb)
	c := RegisterConfig{
		policy:   cache.NewLRUEvictionPolicy(),
		maxBytes: 512,
		maxKeys:  20,
	}

	t1, err := r.Register("test-instance", c)
	if err != nil {
		t.Error(err.Error())
	}

	if i, ok := r.Resolve(t1); i == nil || !ok {
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

	t2, err := r.Register("test-instance-2", c)
	if err != nil {
		t.Error(err.Error())
	}

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

	i1 := r.byToken[t1]
	e := cache.Entry{Key: "test-key"}

	if err := i1.store.Set(&e); err != nil {
		t.Error(err.Error())
	}

	_, err = r.Register("test-instance-1", c)
	if err != nil {
		t.Error(err.Error())
	}

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

	if _, err := r.Register("test-instance-2", c); err == nil {
		t.Error("Expected Register to fail")
	}

	r.Flush(t1)

	if _, err := r.Register("test-instance-2", c); err != nil {
		t.Error(err.Error())
	}
}
