package dynamodb

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/dynamo"
	"github.com/artie-labs/reader/lib/iterator"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/lib/s3lib"
	"github.com/artie-labs/reader/lib/writer"
)

type SnapshotStore struct {
	tableName string
	streamArn string
	cfg       *config.DynamoDB

	s3Client       *s3lib.S3Client
	dynamoDBClient *dynamodb.DynamoDB
}

func (s *SnapshotStore) Close() error {
	return nil
}

func (s *SnapshotStore) Run(ctx context.Context, _writer writer.Writer) error {
	if err := s.scanFilesOverBucket(); err != nil {
		return fmt.Errorf("scanning files over bucket failed: %w", err)
	}

	if err := s.streamAndPublish(ctx, _writer); err != nil {
		return fmt.Errorf("stream and publish failed: %w", err)
	}

	slog.Info("Finished snapshotting all the files")
	return nil
}

func (s *SnapshotStore) scanFilesOverBucket() error {
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

func (s *SnapshotStore) streamAndPublish(ctx context.Context, _writer writer.Writer) error {
	keys, err := s.retrievePrimaryKeys()
	if err != nil {
		return fmt.Errorf("failed to retrieve primary keys: %w", err)
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

		// TODO: Create an actual iterator over the files that is passed to the writer.
		if _, err := _writer.Write(ctx, iterator.SingleBatchIterator(messages)); err != nil {
			return fmt.Errorf("failed to publish messages: %w", err)
		}

		slog.Info("Successfully processed file...", logFields...)
	}

	return nil
}
