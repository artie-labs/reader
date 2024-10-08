package stream

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/sources/dynamodb/offsets"
	"github.com/artie-labs/reader/writers"
)

const (
	jitterSleepBaseMs    = 100
	shardScannerInterval = 5 * time.Minute
)

type Store struct {
	tableName string
	streamArn string
	cfg       *config.DynamoDB

	streams   *dynamodbstreams.Client
	storage   *offsets.OffsetStorage
	shardChan chan types.Shard
}

func NewStore(cfg config.DynamoDB, awsCfg aws.Config) *Store {
	return &Store{
		tableName: cfg.TableName,
		streamArn: cfg.StreamArn,
		cfg:       &cfg,
		streams:   dynamodbstreams.NewFromConfig(awsCfg),
		storage:   offsets.NewStorage(cfg.OffsetFile, nil, nil),
		shardChan: make(chan types.Shard),
	}
}

func (s *Store) Close() error {
	return nil
}

func (s *Store) Run(ctx context.Context, writer writers.Writer) error {
	ticker := time.NewTicker(shardScannerInterval)

	// Start to subscribe to the channel
	go s.ListenToChannel(ctx, writer)

	// Scan it for the first time manually, so we don't have to wait 5 mins
	if err := s.scanForNewShards(ctx); err != nil {
		return fmt.Errorf("failed to scan for new shards: %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			close(s.shardChan)
			slog.Info("Terminating process...")
			return nil
		case <-ticker.C:
			slog.Info("Scanning for new shards...")
			if err := s.scanForNewShards(ctx); err != nil {
				return fmt.Errorf("failed to scan for new shards: %w", err)
			}
		}
	}
}

func (s *Store) scanForNewShards(ctx context.Context) error {
	var exclusiveStartShardId *string
	for {
		input := &dynamodbstreams.DescribeStreamInput{
			StreamArn:             aws.String(s.streamArn),
			ExclusiveStartShardId: exclusiveStartShardId,
		}

		result, err := s.streams.DescribeStream(ctx, input)
		if err != nil {
			return fmt.Errorf("failed to describe stream: %w", err)
		}

		// We need two loops because we need to mark all the shards as "SEEN" before we process.
		for _, shard := range result.StreamDescription.Shards {
			s.storage.SetShardSeen(*shard.ShardId)
		}

		for _, shard := range result.StreamDescription.Shards {
			s.shardChan <- shard
		}

		if result.StreamDescription.LastEvaluatedShardId == nil {
			slog.Info("Finished reading all the shards")
			// If LastEvaluatedShardId is null, we've read all the shards.
			break
		}

		// Set up the next page query with the LastEvaluatedShardId
		exclusiveStartShardId = result.StreamDescription.LastEvaluatedShardId
	}
	return nil
}
