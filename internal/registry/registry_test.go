package registry

import (
	"errors"
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
		PolicyFactory: func() cache.EvictionPolicy { return cache.NewLRUEvictionPolicy() },
		MaxBytes:      512,
		MaxKeys:       20,
		ShardCount:    16,
	}

	token, err := r.Register("test-instance", c)
	if err != nil {
		t.Error(err.Error())
	}
	defer r.Remove(token)

	if i, ok := r.Resolve(token); i == nil || !ok {
		t.Error("Expected Resolve to return an instance")
	}
}

func TestRegistry_TokenUniqueness(t *testing.T) {
	gb := budget.NewGlobalBudget(math.MaxInt64)
	r := NewRegistry(gb)
	c := RegisterConfig{
		PolicyFactory: func() cache.EvictionPolicy { return cache.NewLRUEvictionPolicy() },
		MaxBytes:      512,
		MaxKeys:       20,
		ShardCount:    16,
	}

	t1, err := r.Register("test-instance-1", c)
	if err != nil {
		t.Error(err.Error())
	}
	defer r.Remove(t1)

	t2, err := r.Register("test-instance-2", c)
	if err != nil {
		t.Error(err.Error())
	}
	defer r.Remove(t2)

	if t1 == t2 {
		t.Error("Expected tokens to be unique")
	}
}

func TestRegistry_ReRegistration(t *testing.T) {
	gb := budget.NewGlobalBudget(math.MaxInt64)
	r := NewRegistry(gb)
	c := RegisterConfig{
		PolicyFactory: func() cache.EvictionPolicy { return cache.NewLRUEvictionPolicy() },
		MaxBytes:      2048,
		MaxKeys:       20,
		ShardCount:    16,
	}

	t1, err := r.Register("test-instance-1", c)
	if err != nil {
		t.Error(err.Error())
	}
	defer r.Remove(t1)

	i1 := r.byToken[t1]
	e := cache.Entry{Key: "test-key"}

	if err := i1.store.Set(&e); err != nil {
		t.Error(err.Error())
	}

	t2, err := r.Register("test-instance-1", c)
	if err != nil {
		t.Error(err.Error())
	}
	defer r.Remove(t2)

	if _, ok := i1.store.Get(e.Key, false); ok {
		t.Error("Expected Get to fail")
	}
}

func TestRegistry_BudgetRelease(t *testing.T) {
	gb := budget.NewGlobalBudget(2048)
	r := NewRegistry(gb)
	c := RegisterConfig{
		PolicyFactory: func() cache.EvictionPolicy { return cache.NewLRUEvictionPolicy() },
		MaxBytes:      2048,
		MaxKeys:       20,
		ShardCount:    16,
	}

	t1, err := r.Register("test-instance-1", c)
	if err != nil {
		t.Error(err.Error())
	}
	defer r.Remove(t1)

	t2, err := r.Register("test-instance-2", c)
	if err == nil {
		t.Error("Expected Register to fail")
	}
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Errorf("Expected ErrBudgetExhausted, got %v", err)
	}
	defer r.Remove(t2)

	r.Remove(t1)

	t3, err := r.Register("test-instance-2", c)
	if err != nil {
		t.Error(err.Error())
	}
	defer r.Remove(t3)
}

func TestRegistry_Sweep(t *testing.T) {
	gb := budget.NewGlobalBudget(math.MaxInt64)
	r := NewRegistry(gb)
	c := RegisterConfig{
		PolicyFactory: func() cache.EvictionPolicy { return cache.NewLRUEvictionPolicy() },
		MaxBytes:      2048,
		MaxKeys:       20,
		SweepInterval: 50 * time.Millisecond,
		ShardCount:    16,
	}

	token, err := r.Register("test-instance-1", c)
	if err != nil {
		t.Error(err.Error())
	}
	defer r.Remove(token)

	i1 := r.byToken[token]

	e := cache.Entry{Key: "test-key", ExpiresAt: time.Now()}
	if err := i1.store.Set(&e); err != nil {
		t.Error(err.Error())
	}

	time.Sleep(2 * c.SweepInterval)

	if i, ok := i1.store.Get("test-key", false); i != nil || ok {
		t.Error("Expected key to be gone")
	}

	if i1.metrics.ExpiredSwept() == 0 {
		t.Errorf("Expected expired swept to equal 1, got %d", i1.metrics.expiredSwept.Load())
	}
}

func TestRegistry_InvalidConfigReturnsSentinel(t *testing.T) {
	gb := budget.NewGlobalBudget(math.MaxInt64)
	r := NewRegistry(gb)
	c := RegisterConfig{
		PolicyFactory: func() cache.EvictionPolicy { return cache.NewLRUEvictionPolicy() },
		MaxBytes:      2048,
		MaxKeys:       5, // < ShardCount
		ShardCount:    16,
	}

	_, err := r.Register("test-instance", c)
	if err == nil {
		t.Fatal("expected Register to fail with invalid config")
	}
	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig, got %v", err)
	}

	// Budget must have been released, a follow-up registration with a valid config should succeed.
	c.MaxKeys = 20
	token, err := r.Register("test-instance-2", c)
	if err != nil {
		t.Errorf("expected follow-up registration to succeed (budget should have been released), got %v", err)
	}
	defer r.Remove(token)
}

func TestRegistry_SweepStopsOnContextCancel(t *testing.T) {
	gb := budget.NewGlobalBudget(math.MaxInt64)
	r := NewRegistry(gb)
	c := RegisterConfig{
		PolicyFactory: func() cache.EvictionPolicy { return cache.NewLRUEvictionPolicy() },
		MaxBytes:      2048,
		MaxKeys:       20,
		SweepInterval: 50 * time.Millisecond,
		ShardCount:    16,
	}

	token, _ := r.Register("test-instance", c)
	r.Remove(token)

	goleak.VerifyNone(t)
}
