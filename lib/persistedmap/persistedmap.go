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

type PersistedMap struct {
	filePath    string
	shouldSave  bool
	mu          sync.RWMutex
	data        map[string]any
	flushTicker *time.Ticker
}

func NewPersistedMap(filePath string) *PersistedMap {
	persistedMap := &PersistedMap{
		filePath: filePath,
		data:     make(map[string]any),
	}

	data, err := loadFromFile(filePath)
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

func (p *PersistedMap) Set(key string, value any) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.data[key] = value
	p.shouldSave = true
}

func (p *PersistedMap) Get(key string) (any, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	value, isOk := p.data[key]
	return value, isOk
}

func (p *PersistedMap) flushRoutine() {
	for range p.flushTicker.C {
		if err := p.flush(); err != nil {
			logger.Panic("Failed to flush data", slog.Any("err", err))
		}
	}
}

func (p *PersistedMap) flush() error {
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

func loadFromFile(filePath string) (map[string]any, error) {
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

	var data map[string]any
	if err = yaml.Unmarshal(readBytes, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal data: %w", err)
	}

	return data, nil
}
