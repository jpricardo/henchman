package registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"sync"

	"github.com/jpricardo/henchman/internal/budget"
	"github.com/jpricardo/henchman/internal/cache"
)

type InstanceMetrics struct {
	mu          sync.RWMutex
	expiredKeys int64
}

type Instance struct {
	id             string
	token          string
	store          *cache.Store
	ctx            context.Context
	cancel         context.CancelFunc
	allocatedBytes int64
	sweepInterval  time.Duration
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

type RegisterConfig struct {
	policy        cache.EvictionPolicy
	maxBytes      int64
	maxKeys       int64
	sweepInterval time.Duration
}

func (r *Registry) Register(id string, config RegisterConfig) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if i, exists := r.byID[id]; exists {
		r.remove(i.token)
	}

	err := r.gb.Reserve(config.maxBytes)
	if err != nil {
		return "", err
	}

	s := cache.NewStore(config.policy, config.maxBytes, config.maxKeys)

	t, err := generateRandomHex(16)
	if err != nil {
		r.gb.Release(config.maxBytes)
		return "", err
	}

	ctx, cancel := context.WithCancel(context.Background())

	i := Instance{
		id:             id,
		token:          t,
		store:          s,
		ctx:            ctx,
		cancel:         cancel,
		allocatedBytes: config.maxBytes,
		sweepInterval:  config.sweepInterval,
		metrics:        &InstanceMetrics{},
	}

	r.byID[i.id] = &i
	r.byToken[i.token] = &i

	go sweep(ctx, s, i.metrics, i.sweepInterval)

	return i.token, nil
}

func (r *Registry) Flush(token string) {
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

func (m *InstanceMetrics) ExpiredKeys() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.expiredKeys
}

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
			metrics.mu.Lock()
			metrics.expiredKeys += e
			metrics.mu.Unlock()
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
