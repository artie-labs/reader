package ttlmap

import (
	"context"
	"fmt"
	"gopkg.in/yaml.v2"
	"io"
	"os"
	"sync"
	"time"

	"github.com/artie-labs/reader/lib/logger"
)

const (
	DefaultCleanUpInterval = 5 * time.Minute
	DefaultFlushInterval   = 30 * time.Second
)

type ItemWrapper struct {
	Value            interface{} `yaml:"value"`
	Expiration       int64       `yaml:"expiration"`
	DoNotFlushToDisk bool        `yaml:"-"`
}

type TTLMap struct {
	shouldSave    bool
	ctx           context.Context
	mu            sync.RWMutex
	data          map[string]*ItemWrapper `yaml:"data"`
	filePath      string
	closeChan     chan struct{}
	cleanupTicker *time.Ticker
	flushTicker   *time.Ticker
}

func NewMap(ctx context.Context, filePath string, cleanupInterval, flushInterval time.Duration) *TTLMap {
	t := &TTLMap{
		ctx:       ctx,
		data:      make(map[string]*ItemWrapper),
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

type SetArgs struct {
	Key              string
	Value            interface{}
	DoNotFlushToDisk bool
}

func (t *TTLMap) Set(setArgs SetArgs, ttl time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	expiration := time.Now().Add(ttl).UnixNano()
	t.data[setArgs.Key] = &ItemWrapper{
		Value:            setArgs.Value,
		Expiration:       expiration,
		DoNotFlushToDisk: setArgs.DoNotFlushToDisk,
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

func (t *TTLMap) flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.shouldSave {
		return nil
	}

	file, err := os.Create(t.filePath)
	if err != nil {
		return fmt.Errorf("failed to create file, err: %v", err)
	}

	dataToSave := make(map[string]*ItemWrapper)
	for key, val := range t.data {
		if val.DoNotFlushToDisk {
			continue
		}

		dataToSave[key] = val
	}

	yamlBytes, err := yaml.Marshal(dataToSave)
	if err != nil {
		return fmt.Errorf("failed to marshal data, err: %v", err)
	}

	if _, err = file.Write(yamlBytes); err != nil {
		return fmt.Errorf("failed to write to file, err: %v", err)
	}

	defer file.Close()
	t.shouldSave = false
	return nil
}

func (t *TTLMap) loadFromFile() error {
	file, err := os.Open(t.filePath)
	if err != nil {
		return err
	}

	defer file.Close()

	readBytes, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read file, err: %v", err)
	}

	var data map[string]*ItemWrapper
	if err = yaml.Unmarshal(readBytes, &data); err != nil {
		return fmt.Errorf("failed to unmarshal data, err: %v", err)
	}

	if data == nil {
		data = make(map[string]*ItemWrapper)
	}

	t.mu.Lock()
	t.data = data
	t.mu.Unlock()
	return nil
}
