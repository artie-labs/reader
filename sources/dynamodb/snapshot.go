package dynamodb

import (
	"context"
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
		return fmt.Errorf("failed to list files, err: %v", err)
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

func (s *Store) streamAndPublish(ctx context.Context) error {
	keys, err := s.retrievePrimaryKeys()
	if err != nil {
		return fmt.Errorf("failed to retrieve primary keys, err: %v", err)
	}

	for _, file := range s.cfg.SnapshotSettings.SpecifiedFiles {
		logFields := []any{
			slog.String("fileName", *file.Key),
		}

		slog.Info("Processing file...", logFields...)
		ch := make(chan dynamodb.ItemResponse)
		go func() {
			if err := s.s3Client.StreamJsonGzipFile(file, ch); err != nil {
				logger.Panic("Failed to read file", slog.Any("err", err))
			}
		}()

		var messages []lib.RawMessage
		for msg := range ch {
			dynamoMsg, err := dynamo.NewMessageFromExport(msg, keys, s.tableName)
			if err != nil {
				return fmt.Errorf("failed to cast message from DynamoDB, msg: %v, err: %w", msg, err)
			}
			messages = append(messages, dynamoMsg.RawMessage())
		}

		if err = s.writer.WriteRawMessages(ctx, messages); err != nil {
			return fmt.Errorf("failed to publish messages, err: %w", err)
		}

		slog.Info("Successfully processed file...", logFields...)
	}

	return nil
}
