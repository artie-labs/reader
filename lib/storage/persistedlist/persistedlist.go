package persistedlist

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/artie-labs/reader/lib/logger"
)

type PersistedList[T any] struct {
	file     *os.File
	filePath string
}

func NewPersistedList[T any](filePath string) (PersistedList[T], error) {
	// If the file doesn't exist, create it
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return PersistedList[T]{}, fmt.Errorf("failed to open file")
	}

	return PersistedList[T]{file: file, filePath: filePath}, nil
}

func (p PersistedList[T]) Push(item T) error {
	bytes, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("failed to marshal data")
	}

	bytes = append(bytes, '\n')
	if _, err = p.file.Write(bytes); err != nil {
		return fmt.Errorf("failed to write to file")
	}

	return nil
}

// GetData - This is a separate function since we don't need to keep the entire list in memory
func (p PersistedList[T]) GetData() []T {
	data, err := loadFromFile[T](p.filePath)
	if err != nil {
		logger.Panic("Failed to load persisted map from filepath", slog.Any("err", err))
	}

	return data
}

func loadFromFile[T any](filePath string) ([]T, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	defer file.Close()

	var data []T
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var t T
		if err = json.Unmarshal(scanner.Bytes(), &t); err != nil {
			return nil, fmt.Errorf("failed to unmarshal data: %w", err)
		}

		data = append(data, t)
	}

	return data, nil
}
