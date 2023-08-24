package dynamodb

import (
	"context"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/dynamo"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/sources/dynamodb/offsets"
	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/segmentio/kafka-go"
	"time"
)

type Store struct {
	tableName string
	streamArn string
	batchSize int
	streams   *dynamodbstreams.DynamoDBStreams
	storage   *offsets.OffsetStorage
}

// jitterSleepBaseMs - sleep for 50 ms as the base.
const jitterSleepBaseMs = 50

func Load(ctx context.Context) *Store {
	cfg := config.FromContext(ctx)
	sess, err := session.NewSession(&aws.Config{
		Region: ptr.ToString(cfg.DynamoDB.AwsRegion),
	})

	if err != nil {
		logger.FromContext(ctx).Fatalf("Failed to create session: %v", err)
	}

	store := &Store{
		tableName: cfg.DynamoDB.TableName,
		streamArn: cfg.DynamoDB.StreamArn,
		batchSize: cfg.Kafka.PublishSize,
		storage:   offsets.NewStorage(ctx, cfg.DynamoDB.OffsetFile),
		streams:   dynamodbstreams.New(sess),
	}

	return store
}

func (s *Store) Run(ctx context.Context) {
	log := logger.FromContext(ctx)
	var attempts int
	for {
		input := &dynamodbstreams.DescribeStreamInput{StreamArn: aws.String(s.streamArn)}
		result, err := s.streams.DescribeStream(input)
		if err != nil {
			log.Fatalf("Failed to describe stream: %v", err)
		}

		for _, shard := range result.StreamDescription.Shards {
			if s.storage.GetShardProcessed(*shard.ShardId) {
				logger.FromContext(ctx).WithField("shardId", *shard.ShardId).Info("shard has been processed, skipping...")
				continue
			}

			iteratorType := "TRIM_HORIZON"
			var startingSequenceNumber string
			if seqNumber, exists := s.storage.ReadOnlyLastProcessedSequenceNumbers(*shard.ShardId); exists {
				iteratorType = "AFTER_SEQUENCE_NUMBER"
				startingSequenceNumber = seqNumber
			}

			iteratorInput := &dynamodbstreams.GetShardIteratorInput{
				StreamArn:         ptr.ToString(s.streamArn),
				ShardId:           shard.ShardId,
				ShardIteratorType: ptr.ToString(iteratorType),
			}

			if startingSequenceNumber != "" {
				iteratorInput.SequenceNumber = ptr.ToString(startingSequenceNumber)
			}

			iteratorOutput, err := s.streams.GetShardIterator(iteratorInput)
			if err != nil {
				log.WithError(err).WithFields(map[string]interface{}{
					"streamArn": s.streamArn,
					"shardId":   *shard.ShardId,
				}).Warn("failed to get shard iterator...")
				continue
			}

			shardIterator := iteratorOutput.ShardIterator
			// Get records from shard iterator
			for shardIterator != nil {
				getRecordsInput := &dynamodbstreams.GetRecordsInput{
					ShardIterator: shardIterator,
					Limit:         ptr.ToInt64(1000),
				}

				getRecordsOutput, err := s.streams.GetRecords(getRecordsInput)
				if err != nil {
					log.WithError(err).WithFields(map[string]interface{}{
						"streamArn": s.streamArn,
						"shardId":   *shard.ShardId,
					}).Warn("failed to get records from shard iterator...")
					break
				}

				var messages []kafka.Message
				for _, record := range getRecordsOutput.Records {
					msg, err := dynamo.NewMessage(record, s.tableName)
					if err != nil {
						log.WithError(err).WithFields(map[string]interface{}{
							"streamArn": s.streamArn,
							"shardId":   *shard.ShardId,
							"record":    record,
						}).Fatal("failed to cast message from DynamoDB")
					}

					message, err := msg.KafkaMessage(ctx)
					if err != nil {
						log.WithError(err).WithFields(map[string]interface{}{
							"streamArn": s.streamArn,
							"shardId":   *shard.ShardId,
							"record":    record,
						}).Fatal("failed to cast message from DynamoDB")
					}

					messages = append(messages, message)
				}

				if err = kafkalib.NewBatch(messages, s.batchSize).Publish(ctx); err != nil {
					log.WithError(err).Fatalf("failed to publish messages, exiting...")
				}

				if len(getRecordsOutput.Records) > 0 {
					attempts = 0
					lastRecord := getRecordsOutput.Records[len(getRecordsOutput.Records)-1]
					s.storage.SetLastProcessedSequenceNumber(*shard.ShardId, *lastRecord.Dynamodb.SequenceNumber)
				} else {
					attempts += 1
					sleepDuration := time.Duration(jitter.JitterMs(jitterSleepBaseMs, attempts)) * time.Millisecond
					time.Sleep(sleepDuration)
				}

				shardIterator = getRecordsOutput.NextShardIterator
				if shardIterator == nil {
					// This means this shard has been fully processed, let's add it to our processed list.
					logger.FromContext(ctx).WithField("shardId", *shard.ShardId).Info("shard has been fully processed, adding it to the processed list...")
					s.storage.SetShardProcessed(*shard.ShardId)
				}
			}
		}
	}
}
