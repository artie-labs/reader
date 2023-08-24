package ttlmap

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/artie-labs/reader/lib/logger"
	"os"
	"sync"
	"time"
)

const (
	DefaultCleanUpInterval = 5 * time.Minute
	DefaultFlushInterval   = 30 * time.Second
)

type itemWrapper struct {
	Value      interface{}
	Expiration int64
}

type TTLMap struct {
	shouldSave    bool
	ctx           context.Context
	mu            sync.RWMutex
	data          map[string]*itemWrapper
	filePath      string
	closeChan     chan struct{}
	cleanupTicker *time.Ticker
	flushTicker   *time.Ticker
}

func NewMap(ctx context.Context, filePath string, cleanupInterval, flushInterval time.Duration) *TTLMap {
	t := &TTLMap{
		ctx:       ctx,
		data:      make(map[string]*itemWrapper),
		filePath:  filePath,
		closeChan: make(chan struct{}),
	}

	if err := t.loadFromFile(); err != nil {
		logger.FromContext(ctx).WithError(err).Warn("failed to load ttlmap from memory, starting a new one...")
	}

	t.cleanupTicker = time.NewTicker(cleanupInterval)
	t.flushTicker = time.NewTicker(flushInterval)

	go t.cleanUpAndFlushRoutine()

	return t
}

func (t *TTLMap) Set(key string, value interface{}, ttl time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	expiration := time.Now().Add(ttl).UnixNano()
	t.data[key] = &itemWrapper{
		Value:      value,
		Expiration: expiration,
	}

	t.shouldSave = true
}

func (t *TTLMap) Get(key string) (interface{}, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	item, exists := t.data[key]
	if !exists || time.Now().UnixNano() > item.Expiration {
		return nil, false
	}

	return item.Value, true
}

func (t *TTLMap) cleanUpAndFlushRoutine() {
	for {
		select {
		case <-t.cleanupTicker.C:
			t.cleanup()
		case <-t.flushTicker.C:
			if err := t.flush(); err != nil {
				logger.FromContext(t.ctx).WithError(err).Fatal("failed to flush")
			}
		case <-t.closeChan:
			return
		}
	}
}

func (t *TTLMap) cleanup() {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now().UnixNano()
	for k, v := range t.data {
		if now > v.Expiration {
			delete(t.data, k)
			t.shouldSave = true
		}
	}
}

func (t *TTLMap) Close() {
	t.cleanupTicker.Stop()
	t.flushTicker.Stop()
	close(t.closeChan)
}

func (t *TTLMap) flush() error {
	file, err := os.Create(t.filePath)
	if err != nil {
		return fmt.Errorf("failed to create file, err: %v", err)
	}

	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err = encoder.Encode(t.data); err != nil {
		return err
	}

	t.shouldSave = false
	return nil
}

func (t *TTLMap) loadFromFile() error {
	file, err := os.Open(t.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	defer file.Close()

	decoder := gob.NewDecoder(file)
	return decoder.Decode(&t.data)
}
