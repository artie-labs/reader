package dynamodb

import (
	"context"
	"fmt"
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
		logger.FromContext(ctx).WithField("fileName", *file.Key).Info("discovered file, adding to the processing queue...")
	}

	s.cfg.SnapshotSettings.SpecifiedFiles = files
	return nil
}

func (s *Store) streamAndPublish(ctx context.Context) error {
	log := logger.FromContext(ctx)

	keys, err := s.retrievePrimaryKeys()
	if err != nil {
		return fmt.Errorf("failed to retrieve primary keys, err: %v", err)
	}

	for _, file := range s.cfg.SnapshotSettings.SpecifiedFiles {
		logFields := map[string]interface{}{
			"fileName": *file.Key,
		}

		log.WithFields(logFields).Info("processing file...")
		ch := make(chan dynamodb.ItemResponse)
		go func() {
			if err := s.s3Client.StreamJsonGzipFile(file, ch); err != nil {
				log.Fatalf("Failed to read file: %v", err)
			}
		}()

		var kafkaMsgs []kafka.Message
		for msg := range ch {
			dynamoMsg, err := dynamo.NewMessageFromExport(msg, keys, s.tableName)
			if err != nil {
				log.WithError(err).WithFields(map[string]interface{}{
					"msg": msg,
				}).Fatal("failed to cast message from DynamoDB")
			}

			kafkaMsg, err := dynamoMsg.KafkaMessage(ctx)
			if err != nil {
				log.WithError(err).Fatal("failed to cast message from DynamoDB")
			}

			kafkaMsgs = append(kafkaMsgs, kafkaMsg)
		}

		if err = kafkalib.NewBatch(kafkaMsgs, s.batchSize).Publish(ctx); err != nil {
			log.WithError(err).Fatalf("failed to publish messages, exiting...")
		}

		log.WithFields(logFields).Info("successfully processed file...")
	}

	return nil
}
