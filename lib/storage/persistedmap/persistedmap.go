package persistedmap

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/artie-labs/reader/lib/logger"
)

type PersistedMap[T any] struct {
	filePath    string
	shouldSave  bool
	mu          sync.RWMutex
	data        map[string]T
	flushTicker *time.Ticker
}

func NewPersistedMap[T any](filePath string) *PersistedMap[T] {
	persistedMap := &PersistedMap[T]{
		filePath: filePath,
		data:     make(map[string]T),
	}

	data, err := loadFromFile[T](filePath)
	if err != nil {
		logger.Panic("Failed to load persisted map from filepath", slog.Any("err", err))
	}

	if len(data) > 0 {
		persistedMap.data = data
	}

	persistedMap.flushTicker = time.NewTicker(30 * time.Second)
	go persistedMap.flushRoutine()

	return persistedMap
}

func (p *PersistedMap[T]) Set(key string, value T) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.data[key] = value
	p.shouldSave = true
}

func (p *PersistedMap[T]) Get(key string) (T, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	value, isOk := p.data[key]
	return value, isOk
}

func (p *PersistedMap[T]) flushRoutine() {
	for range p.flushTicker.C {
		if err := p.flush(); err != nil {
			logger.Panic("Failed to flush data", slog.Any("err", err))
		}
	}
}

func (p *PersistedMap[T]) flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.shouldSave {
		return nil
	}

	file, err := os.Create(p.filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}

	defer file.Close()

	yamlBytes, err := yaml.Marshal(p.data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	if _, err = file.Write(yamlBytes); err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}

	p.shouldSave = false
	return nil
}

func loadFromFile[T any](filePath string) (map[string]T, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	defer file.Close()
	readBytes, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var data map[string]T
	if err = yaml.Unmarshal(readBytes, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal data: %w", err)
	}

	return data, nil
}
