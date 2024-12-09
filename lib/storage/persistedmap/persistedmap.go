package persistedmap

import (
	"fmt"
	"github.com/artie-labs/reader/lib/logger"
	"gopkg.in/yaml.v3"
	"io"
	"log/slog"
	"os"
)

type PersistedMap[T any] struct {
	filePath string
	data     map[string]T
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

	return persistedMap
}

func (p *PersistedMap[T]) Set(key string, value T) error {
	p.data[key] = value
	
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

	return file.Close()
}

func (p *PersistedMap[T]) Get(key string) (T, bool) {
	value, isOk := p.data[key]
	return value, isOk
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
