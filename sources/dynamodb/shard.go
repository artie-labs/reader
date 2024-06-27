package dynamodb

import (
	"context"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/dynamo"
	"github.com/artie-labs/reader/lib/iterator"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/writers"
)

func (s *StreamStore) ListenToChannel(ctx context.Context, writer writers.Writer) {
	for shard := range s.shardChan {
		go s.processShard(ctx, shard, writer)
	}
}

func (s *StreamStore) processShard(ctx context.Context, shard *dynamodbstreams.Shard, writer writers.Writer) {
	// Is there another go-routine processing this shard?
	if s.storage.GetShardProcessing(*shard.ShardId) {
		return
	}

	if parentID := shard.ParentShardId; parentID != nil {
		// If the parent shard exists, is it still being processed? If so, let's wait a bit and then retry.
		// We must process the parent shard first before processing the child shard.
		if s.storage.GetShardProcessing(*parentID) && !s.storage.GetShardProcessed(*parentID) {
			slog.Info("Parent shard is being processed, let's sleep 3s and retry", slog.String("shardId", *shard.ShardId), slog.String("parentShardId", *parentID))
			time.Sleep(3 * time.Second)
			s.processShard(ctx, shard, writer)
			return
		}
	}

	// If no one is processing it, let's mark it as being processed.
	s.storage.SetShardProcessing(*shard.ShardId)
	if s.storage.GetShardProcessed(*shard.ShardId) {
		slog.Info("Shard has been processed, skipping...", slog.String("shardId", *shard.ShardId))
		return
	}

	slog.Info("Processing shard...", slog.String("shardId", *shard.ShardId))

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
		slog.Warn("Failed to get shard iterator...",
			slog.Any("err", err),
			slog.String("streamArn", s.streamArn),
			slog.String("shardId", *shard.ShardId),
		)
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
			slog.Warn("Failed to get records from shard iterator...",
				slog.Any("err", err),
				slog.String("streamArn", s.streamArn),
				slog.String("shardId", *shard.ShardId),
			)
			break
		}

		var messages []lib.RawMessage
		for _, record := range getRecordsOutput.Records {
			msg, err := dynamo.NewMessage(record, s.tableName)
			if err != nil {
				logger.Panic("Failed to cast message from DynamoDB",
					slog.Any("err", err),
					slog.String("streamArn", s.streamArn),
					slog.String("shardId", *shard.ShardId),
					slog.Any("record", record),
				)
			}
			messages = append(messages, msg.RawMessage())
		}

		// TODO: Create an actual iterator over the shards that is passed to the writer.
		if _, err = writer.Write(ctx, iterator.Once(messages)); err != nil {
			logger.Panic("Failed to publish messages, exiting...", slog.Any("err", err))
		}

		var attempts int
		if len(getRecordsOutput.Records) > 0 {
			attempts = 0
			lastRecord := getRecordsOutput.Records[len(getRecordsOutput.Records)-1]
			s.storage.SetLastProcessedSequenceNumber(*shard.ShardId, *lastRecord.Dynamodb.SequenceNumber)
		} else {
			attempts += 1
		}

		time.Sleep(jitter.Jitter(jitterSleepBaseMs, jitter.DefaultMaxMs, attempts))

		shardIterator = getRecordsOutput.NextShardIterator
		if shardIterator == nil {
			// This means this shard has been fully processed, let's add it to our processed list.
			slog.Info("Shard has been fully processed, adding it to the processed list...", slog.String("shardId", *shard.ShardId))
			s.storage.SetShardProcessed(*shard.ShardId)
		}
	}
}
