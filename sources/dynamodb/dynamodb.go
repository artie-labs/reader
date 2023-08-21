package dynamodb

import (
	"context"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/dynamo"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/segmentio/kafka-go"
	"time"
)

type Store struct {
	tableName               string
	streamArn               string
	offsetFilePath          string
	batchSize               int
	lastProcessedSeqNumbers map[string]string
	streams                 *dynamodbstreams.DynamoDBStreams
}

const (
	flushOffsetInterval = 30 * time.Second
	// jitterSleepBaseMs - sleep for 5s as the base.
	jitterSleepBaseMs = 5000
)

func Load(ctx context.Context) *Store {
	cfg := config.FromContext(ctx)
	sess, err := session.NewSession(&aws.Config{
		Region: ptr.ToString(cfg.DynamoDB.AwsRegion),
	})

	if err != nil {
		logger.FromContext(ctx).Fatalf("Failed to create session: %v", err)
	}

	store := &Store{
		tableName:               cfg.DynamoDB.TableName,
		streamArn:               cfg.DynamoDB.StreamArn,
		offsetFilePath:          cfg.DynamoDB.OffsetFile,
		batchSize:               cfg.Kafka.PublishSize,
		lastProcessedSeqNumbers: make(map[string]string),
		streams:                 dynamodbstreams.New(sess),
	}

	store.loadOffsets(ctx)
	return store
}

func (s *Store) Run(ctx context.Context) {
	ticker := time.NewTicker(flushOffsetInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				s.saveOffsets(ctx)
			}
		}
	}()

	log := logger.FromContext(ctx)
	var retrievedMessages bool
	var attempts int

	for {
		input := &dynamodbstreams.DescribeStreamInput{StreamArn: aws.String(s.streamArn)}
		result, err := s.streams.DescribeStream(input)
		if err != nil {
			log.Fatalf("Failed to describe stream: %v", err)
		}

		for shardCount, shard := range result.StreamDescription.Shards {
			iteratorType := "TRIM_HORIZON"
			var startingSequenceNumber string

			if seqNumber, exists := s.lastProcessedSeqNumbers[*shard.ShardId]; exists {
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
					retrievedMessages = true
					lastRecord := getRecordsOutput.Records[len(getRecordsOutput.Records)-1]
					s.lastProcessedSeqNumbers[*shard.ShardId] = *lastRecord.Dynamodb.SequenceNumber
				} else {
					// Don't break if it's not the last shard because then we'll skip over the iteration.
					if shardCount == len(result.StreamDescription.Shards)-1 {
						break
					}
				}

				shardIterator = getRecordsOutput.NextShardIterator
			}
		}

		if !retrievedMessages {
			attempts += 1
			sleepDuration := time.Duration(jitter.JitterMs(jitterSleepBaseMs, attempts)) * time.Millisecond
			log.WithFields(map[string]interface{}{
				"streamArn":     s.streamArn,
				"sleepDuration": sleepDuration,
				"attempts":      attempts,
			}).Info("No messages retrieved this iteration, sleeping and will retry again")

			time.Sleep(sleepDuration)
		} else {
			attempts = 0
		}
	}
}
