package throttler

import (
	"fmt"
	"sync"
)

type Throttler struct {
	limit   int64
	running int64
	mu      sync.Mutex
}

func NewThrottler(limit int64) (*Throttler, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("throttler limit should be greater than 0")
	}

	return &Throttler{limit: limit}, nil
}

func (t *Throttler) Start() {
	t.mu.Lock()
	t.running++
	t.mu.Unlock()
}

func (t *Throttler) Done() {
	t.mu.Lock()
	t.running--
	t.mu.Unlock()
}

func (t *Throttler) Allowed() bool {
	return t.running < t.limit
}
