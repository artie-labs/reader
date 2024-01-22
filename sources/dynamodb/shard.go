package dynamodb

import (
	"context"
	"log/slog"
	"time"

	"github.com/artie-labs/reader/lib/dynamo"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/segmentio/kafka-go"
)

func (s *Store) ListenToChannel(ctx context.Context) {
	for shard := range s.shardChan {
		go s.processShard(ctx, shard)
	}
}

func (s *Store) processShard(ctx context.Context, shard *dynamodbstreams.Shard) {
	var attempts int

	// Is there another go-routine processing this shard?
	if s.storage.GetShardProcessing(*shard.ShardId) {
		return
	}

	// If no one is processing it, let's mark it as being processed.
	s.storage.SetShardProcessing(*shard.ShardId)
	if s.storage.GetShardProcessed(*shard.ShardId) {
		slog.With("shardId", *shard.ShardId).Info("shard has been processed, skipping...")
		return
	}

	slog.With("shardId", *shard.ShardId).Info("processing shard...")

	iteratorType := "TRIM_HORIZON"
	var startingSequenceNumber string
	if seqNumber, exists := s.storage.LastProcessedSequenceNumber(*shard.ShardId); exists {
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
		slog.With(
			slog.Any("err", err),
			slog.String("streamArn", s.streamArn),
			slog.String("shardId", *shard.ShardId),
		).Warn("failed to get shard iterator...")
		return
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
			slog.With(
				slog.Any("err", err),
				slog.String("streamArn", s.streamArn),
				slog.String("shardId", *shard.ShardId),
			).Warn("failed to get records from shard iterator...")
			break
		}

		var messages []kafka.Message
		for _, record := range getRecordsOutput.Records {
			msg, err := dynamo.NewMessage(record, s.tableName)
			if err != nil {
				logger.Fatal("failed to cast message from DynamoDB",
					slog.Any("err", err),
					slog.String("streamArn", s.streamArn),
					slog.String("shardId", *shard.ShardId),
					slog.Any("record", record),
				)
			}

			message, err := msg.KafkaMessage(ctx)
			if err != nil {
				logger.Fatal("failed to cast message from DynamoDB",
					slog.Any("err", err),
					slog.String("streamArn", s.streamArn),
					slog.String("shardId", *shard.ShardId),
					slog.Any("record", record),
				)
			}

			messages = append(messages, message)
		}

		if err = kafkalib.NewBatch(messages, s.batchSize).Publish(ctx); err != nil {
			logger.Fatal("failed to publish messages, exiting...", slog.Any("err", err))
		}

		if len(getRecordsOutput.Records) > 0 {
			attempts = 0
			lastRecord := getRecordsOutput.Records[len(getRecordsOutput.Records)-1]
			s.storage.SetLastProcessedSequenceNumber(*shard.ShardId, *lastRecord.Dynamodb.SequenceNumber)
		} else {
			attempts += 1
		}

		sleepDuration := time.Duration(jitter.JitterMs(jitterSleepBaseMs, attempts)) * time.Millisecond
		time.Sleep(sleepDuration)

		shardIterator = getRecordsOutput.NextShardIterator
		if shardIterator == nil {
			// This means this shard has been fully processed, let's add it to our processed list.
			slog.With("shardId", *shard.ShardId).Info("shard has been fully processed, adding it to the processed list...")
			s.storage.SetShardProcessed(*shard.ShardId)
		}
	}
}
