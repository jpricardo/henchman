package budget

import (
	"fmt"
	"sync"
)

type GlobalBudget struct {
	mu             sync.Mutex
	totalBytes     int64
	allocatedBytes int64
}

func NewGlobalBudget(totalBytes int64) *GlobalBudget {
	return &GlobalBudget{
		mu:             sync.Mutex{},
		totalBytes:     totalBytes,
		allocatedBytes: 0,
	}
}

func (gb *GlobalBudget) Reserve(bytes int64) error {
	gb.mu.Lock()
	defer gb.mu.Unlock()

	if bytes+gb.allocatedBytes > gb.totalBytes {
		return fmt.Errorf("unable to allocate %d bytes", bytes)
	}

	gb.allocatedBytes += bytes

	return nil
}

func (gb *GlobalBudget) Release(bytes int64) {
	gb.mu.Lock()
	defer gb.mu.Unlock()

	gb.allocatedBytes = max(gb.allocatedBytes-bytes, 0)
}
