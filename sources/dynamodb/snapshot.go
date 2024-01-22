package dynamodb

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/artie-labs/reader/lib/dynamo"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/segmentio/kafka-go"
)

func (s *Store) scanFilesOverBucket(ctx context.Context) error {
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
		slog.With("fileName", *file.Key).Info("discovered file, adding to the processing queue...")
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

		slog.With(logFields...).Info("processing file...")
		ch := make(chan dynamodb.ItemResponse)
		go func() {
			if err := s.s3Client.StreamJsonGzipFile(file, ch); err != nil {
				logger.Fatal("Failed to read file", slog.Any("err", err))
			}
		}()

		var kafkaMsgs []kafka.Message
		for msg := range ch {
			dynamoMsg, err := dynamo.NewMessageFromExport(msg, keys, s.tableName)
			if err != nil {
				logger.Fatal("failed to cast message from DynamoDB", slog.Any("err", err), slog.Any("msg", msg))
			}

			kafkaMsg, err := dynamoMsg.KafkaMessage(ctx)
			if err != nil {
				logger.Fatal("failed to cast message from DynamoDB", slog.Any("err", err))
			}

			kafkaMsgs = append(kafkaMsgs, kafkaMsg)
		}

		if err = kafkalib.NewBatch(kafkaMsgs, s.batchSize).Publish(ctx); err != nil {
			logger.Fatal("failed to publish messages, exiting...")
		}

		slog.With(logFields...).Info("successfully processed file...")
	}

	return nil
}
