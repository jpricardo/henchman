package registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jpricardo/henchman/internal/budget"
	"github.com/jpricardo/henchman/internal/cache"
)

type InstanceMetrics struct {
	hits         atomic.Int64
	misses       atomic.Int64
	expiredSwept atomic.Int64
}

type MetricsSnapshot struct {
	InstanceID   string
	Hits         int64
	Misses       int64
	Evictions    int64
	BytesUsed    int64
	BytesCap     int64
	KeysCurrent  int64
	ExpiredSwept int64
}

type Instance struct {
	id             string
	token          string
	store          *cache.Store
	ctx            context.Context
	cancel         context.CancelFunc
	allocatedBytes int64
	sweepInterval  time.Duration
	defaultTTL     time.Duration
	metrics        *InstanceMetrics
}

type Registry struct {
	mu      sync.RWMutex
	gb      *budget.GlobalBudget
	byID    map[string]*Instance
	byToken map[string]*Instance
}

func NewRegistry(gb *budget.GlobalBudget) *Registry {
	return &Registry{
		mu:      sync.RWMutex{},
		gb:      gb,
		byID:    make(map[string]*Instance),
		byToken: make(map[string]*Instance),
	}
}

// #region Registry

type RegisterConfig struct {
	Policy        cache.EvictionPolicy
	MaxBytes      int64
	MaxKeys       int64
	SweepInterval time.Duration
	DefaultTTL    time.Duration
}

func (r *Registry) Register(id string, config RegisterConfig) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if i, exists := r.byID[id]; exists {
		r.remove(i.token)
	}

	err := r.gb.Reserve(config.MaxBytes)
	if err != nil {
		return "", err
	}

	s := cache.NewStore(config.Policy, config.MaxBytes, config.MaxKeys)

	t, err := generateRandomHex(16)
	if err != nil {
		r.gb.Release(config.MaxBytes)
		return "", err
	}

	ctx, cancel := context.WithCancel(context.Background())

	i := Instance{
		id:             id,
		token:          t,
		store:          s,
		ctx:            ctx,
		cancel:         cancel,
		allocatedBytes: config.MaxBytes,
		sweepInterval:  config.SweepInterval,
		defaultTTL:     config.DefaultTTL,
		metrics:        &InstanceMetrics{},
	}

	r.byID[i.id] = &i
	r.byToken[i.token] = &i

	go sweep(ctx, s, i.metrics, i.sweepInterval)

	return i.token, nil
}

func (r *Registry) Remove(token string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.remove(token)
}

func (r *Registry) remove(token string) {
	if i, exists := r.byToken[token]; exists {
		i.cancel()
		i.store.Flush()
		r.gb.Release(i.allocatedBytes)

		delete(r.byToken, token)
		delete(r.byID, i.id)
	}
}

func (r *Registry) Resolve(token string) (*Instance, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	i, exists := r.byToken[token]
	return i, exists
}

func (r *Registry) Shutdown() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for token := range r.byToken {
		r.remove(token)
	}
}

// #endregion

// #region Instance

func (i *Instance) Get(key string, invalidateMatched bool) (*cache.Entry, bool) {
	e, ok := i.store.Get(key, invalidateMatched)
	if ok {
		i.metrics.hits.Add(1)
	} else {
		i.metrics.misses.Add(1)
	}
	return e, ok
}

func (i *Instance) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		InstanceID:   i.id,
		Hits:         i.metrics.hits.Load(),
		Misses:       i.metrics.misses.Load(),
		Evictions:    i.store.Evictions(),
		BytesUsed:    i.store.CurrentBytes(),
		BytesCap:     i.allocatedBytes,
		KeysCurrent:  i.store.CurrentKeys(),
		ExpiredSwept: i.metrics.expiredSwept.Load(),
	}
}

func (i *Instance) Set(entry *cache.Entry) error {
	err := i.store.Set(entry)

	return err
}

func (i *Instance) Delete(key string) {
	i.store.Delete(key)
}

func (i *Instance) Query(keyPrefix string, invalidateMatched bool) []*cache.Entry {
	return i.store.Query(keyPrefix, invalidateMatched)
}

func (i *Instance) Flush() {
	i.store.Flush()
}

func (i *Instance) Token() string {
	return i.token
}

func (i *Instance) DefaultTTL() time.Duration {
	return i.defaultTTL
}

func (m *InstanceMetrics) ExpiredSwept() int64 {
	return m.expiredSwept.Load()
}

func (r *Registry) Snapshots() []MetricsSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()

	snapshots := make([]MetricsSnapshot, 0, len(r.byID))
	for _, inst := range r.byID {
		snapshots = append(snapshots, inst.Snapshot())
	}
	return snapshots
}

// #endregion

func sweep(ctx context.Context, s *cache.Store, metrics *InstanceMetrics, interval time.Duration) {
	if interval <= 0 {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return

		case <-time.After(interval):
			e := s.Sweep()
			metrics.expiredSwept.Add(e)
		}
	}
}

func generateRandomHex(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
