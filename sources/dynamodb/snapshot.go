package dynamodb

import (
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/dynamo"
	"github.com/artie-labs/reader/lib/logger"
)

func (s *Store) scanFilesOverBucket() error {
	if len(s.cfg.SnapshotSettings.SpecifiedFiles) > 0 {
		// Don't scan because you are already specifying files
		return nil
	}

	files, err := s.s3Client.ListFiles(s.cfg.SnapshotSettings.Folder)
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	if len(files) == 0 {
		return fmt.Errorf("no files found in the folder: %v", s.cfg.SnapshotSettings.Folder)
	}

	for _, file := range files {
		slog.Info("Discovered file, adding to the processing queue...", slog.String("fileName", *file.Key))
	}

	s.cfg.SnapshotSettings.SpecifiedFiles = files
	return nil
}

type iterator struct {
	keys  []string
	index int
	store *Store
}

func (s *Store) NewIterator() (*iterator, error) {
	keys, err := s.retrievePrimaryKeys()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve primary keys: %w", err)
	}

	return &iterator{
		keys:  keys,
		store: s,
	}, nil
}

func (i *iterator) HasNext() bool {
	return i.index < len(i.store.cfg.SnapshotSettings.SpecifiedFiles)
}

func (i *iterator) Next() ([]lib.RawMessage, error) {
	if !i.HasNext() {
		return nil, fmt.Errorf("no more files to scan")
	}

	file := i.store.cfg.SnapshotSettings.SpecifiedFiles[i.index]
	logFields := []any{
		slog.String("fileName", *file.Key),
	}

	slog.Info("Processing file...", logFields...)
	ch := make(chan dynamodb.ItemResponse)
	go func() {
		if err := i.store.s3Client.StreamJsonGzipFile(file, ch); err != nil {
			logger.Panic("Failed to read file", slog.Any("err", err))
		}
	}()

	var messages []lib.RawMessage
	for msg := range ch {
		dynamoMsg, err := dynamo.NewMessageFromExport(msg, i.keys, i.store.tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to cast message from DynamoDB, msg: %v, err: %w", msg, err)
		}
		messages = append(messages, dynamoMsg.RawMessage())
	}

	slog.Info("Successfully processed file...", logFields...)

	i.index++
	return messages, nil
}
