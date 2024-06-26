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

	if err := persistedMap.loadFromFile(); err != nil {
		slog.Warn("Failed to load persisted map from filepath, starting a new one...", slog.Any("err", err))
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
	if !p.shouldSave {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	file, err := os.Create(p.filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}

	yamlBytes, err := yaml.Marshal(p.data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	if _, err = file.Write(yamlBytes); err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}

	defer file.Close()

	p.shouldSave = false
	return nil
}

func (p *PersistedMap) loadFromFile() error {
	file, err := os.Open(p.filePath)
	if err != nil {
		return err
	}

	defer file.Close()

	readBytes, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	var data map[string]any
	if err = yaml.Unmarshal(readBytes, &data); err != nil {
		return fmt.Errorf("failed to unmarshal data: %w", err)
	}

	if data == nil {
		data = make(map[string]any)
	}

	p.mu.Lock()
	p.data = data
	p.mu.Unlock()
	return nil
}
