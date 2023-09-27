package dynamodb

import (
	"context"
	"fmt"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (s *Store) scanFilesOverBucket() error {
	files, err := s.s3Client.ListFiles(s.cfg.SnapshotSettings.Folder)
	if err != nil {
		return fmt.Errorf("failed to list files, err: %v", err)
	}

	if len(files) == 0 {
		return fmt.Errorf("no files found in the folder: %v", s.cfg.SnapshotSettings.Folder)
	}

	s.cfg.SnapshotSettings.SpecifiedFiles = files
	return nil
}

func (s *Store) ReadAndPublish(ctx context.Context) error {
	log := logger.FromContext(ctx)

	for _, file := range s.cfg.SnapshotSettings.SpecifiedFiles {
		ch := make(chan dynamodb.ItemResponse)
		go func() {
			if err := s.s3Client.StreamJsonGzipFile(file, ch); err != nil {
				log.Fatalf("Failed to read file: %v", err)
			}
		}()

		//var kafkaMsgs []kafka.Message
		for msg := range ch {
			fmt.Println("msg", msg)
			//kafkaMsg, err := dynamo.NewMessage(msg, s.tableName)
			//if err != nil {
			//	log.WithError(err).WithFields(map[string]interface{}{
			//		"streamArn": s.streamArn,
			//		"shardId":   *shard.ShardId,
			//		"record":    record,
			//	}).Fatal("failed to cast message from DynamoDB")
			//}
		}
	}

	return nil
}
