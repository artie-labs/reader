package dynamodb

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/dynamo"
	"github.com/artie-labs/reader/lib/iterator"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/writers"
)

const maxNumErrs = 25

func (s *StreamStore) ListenToChannel(ctx context.Context, writer writers.Writer) {
	for shard := range s.shardChan {
		go s.processShard(ctx, shard, writer, 0)
	}
}

<<<<<<< HEAD
func (s *StreamStore) processShard(ctx context.Context, shard types.Shard, writer writers.Writer) {
=======
func (s *StreamStore) reprocessShard(ctx context.Context, shard *dynamodbstreams.Shard, writer writers.Writer, numErrs int, err error) {
	if numErrs > maxNumErrs {
		logger.Panic(fmt.Sprintf("Failed to process shard: %s and the max number of attempts have been reached", *shard.ShardId), err)
	}

	slog.Warn("Failed to process shard, going to try again...",
		slog.Any("err", err),
		slog.String("streamArn", s.streamArn),
		slog.String("shardId", *shard.ShardId),
		slog.Int("numErrs", numErrs),
	)

	// Unset it so we can process it again
	s.storage.UnsetShardProcessing(*shard.ShardId)
	s.processShard(ctx, shard, writer, numErrs+1)
}

func (s *StreamStore) processShard(ctx context.Context, shard *dynamodbstreams.Shard, writer writers.Writer, numErrs int) {
>>>>>>> master
	// Is there another go-routine processing this shard?
	if s.storage.GetShardProcessing(*shard.ShardId) {
		return
	}

	if parentID := shard.ParentShardId; parentID != nil {
		// Have we seen the parent? If so, let's wait for processing to finish
		// If we haven't seen the parent, then we can assume this is the parent, and we don't need to wait.
		if s.storage.GetShardSeen(*parentID) && !s.storage.GetShardProcessed(*parentID) {
			slog.Info("Parent shard is being processed, let's sleep 3s and retry", slog.String("shardId", *shard.ShardId), slog.String("parentShardId", *parentID))
			time.Sleep(3 * time.Second)
			s.processShard(ctx, shard, writer, numErrs)
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

	iteratorType := types.ShardIteratorTypeTrimHorizon
	var startingSequenceNumber string
	if seqNumber, exists := s.storage.LastProcessedSequenceNumber(*shard.ShardId); exists {
		iteratorType = types.ShardIteratorTypeAfterSequenceNumber
		startingSequenceNumber = seqNumber
	}

	iteratorInput := &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         ptr.ToString(s.streamArn),
		ShardId:           shard.ShardId,
		ShardIteratorType: iteratorType,
	}

	if startingSequenceNumber != "" {
		iteratorInput.SequenceNumber = ptr.ToString(startingSequenceNumber)
	}

	iteratorOutput, err := s.streams.GetShardIterator(ctx, iteratorInput)
	if err != nil {
		s.reprocessShard(ctx, shard, writer, numErrs, fmt.Errorf("failed to get shard iterator: %w", err))
		return
	}

	shardIterator := iteratorOutput.ShardIterator
	// Get records from shard iterator
	for shardIterator != nil {
		getRecordsInput := &dynamodbstreams.GetRecordsInput{
			ShardIterator: shardIterator,
			Limit:         ptr.ToInt32(1000),
		}

		getRecordsOutput, err := s.streams.GetRecords(ctx, getRecordsInput)
		if err != nil {
			s.reprocessShard(ctx, shard, writer, numErrs, fmt.Errorf("failed to get records: %w", err))
			return
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
