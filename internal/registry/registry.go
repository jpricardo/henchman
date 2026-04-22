package registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	"sync"

	"github.com/jpricardo/henchman/internal/budget"
	"github.com/jpricardo/henchman/internal/cache"
)

type Instance struct {
	id             string
	token          string
	store          *cache.Store
	ctx            context.Context
	cancel         context.CancelFunc
	allocatedBytes int64
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
	policy   cache.EvictionPolicy
	maxBytes int64
	maxKeys  int64
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
	}

	r.byID[i.id] = &i
	r.byToken[i.token] = &i

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

func generateRandomHex(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
