package throttler

import "sync"

type Throttler struct {
	Limit   int64
	running int64
	mu      sync.Mutex
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
	return t.running < t.Limit
}
