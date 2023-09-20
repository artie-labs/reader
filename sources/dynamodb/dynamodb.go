package dynamodb

import (
	"context"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/sources/dynamodb/offsets"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"time"
)

type Store struct {
	tableName string
	streamArn string
	batchSize int
	streams   *dynamodbstreams.DynamoDBStreams
	storage   *offsets.OffsetStorage
	shardChan chan *dynamodbstreams.Shard
}

// jitterSleepBaseMs - sleep for 50 ms as the base.
const jitterSleepBaseMs = 50
const shardScannerInterval = 5 * time.Minute

func Load(ctx context.Context) *Store {
	cfg := config.FromContext(ctx)

	sess, err := session.NewSession(&aws.Config{
		Region:      ptr.ToString(cfg.DynamoDB.AwsRegion),
		Credentials: credentials.NewStaticCredentials(cfg.DynamoDB.AwsAccessKeyID, cfg.DynamoDB.AwsSecretAccessKey, ""),
	})

	if err != nil {
		logger.FromContext(ctx).Fatalf("Failed to create session: %v", err)
	}

	store := &Store{
		tableName: cfg.DynamoDB.TableName,
		streamArn: cfg.DynamoDB.StreamArn,
		batchSize: cfg.Kafka.PublishSize,
		storage:   offsets.NewStorage(ctx, cfg.DynamoDB.OffsetFile, nil, nil),
		streams:   dynamodbstreams.New(sess),
		shardChan: make(chan *dynamodbstreams.Shard),
	}

	return store
}

func (s *Store) Run(ctx context.Context) {
	ticker := time.NewTicker(shardScannerInterval)

	// Start to subscribe to the channel
	go s.ListenToChannel(ctx)

	// Scan it for the first time manually, so we don't have to wait 5 mins
	s.scanForNewShards(ctx)

	log := logger.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			close(s.shardChan)
			log.Info("Terminating process...")
			return
		case <-ticker.C:
			log.Info("Scanning for new shards...")
			s.scanForNewShards(ctx)
		}
	}

}

func (s *Store) scanForNewShards(ctx context.Context) {
	var exclusiveStartShardId *string
	for {
		input := &dynamodbstreams.DescribeStreamInput{
			StreamArn:             aws.String(s.streamArn),
			ExclusiveStartShardId: exclusiveStartShardId,
		}

		result, err := s.streams.DescribeStream(input)
		if err != nil {
			logger.FromContext(ctx).Fatalf("Failed to describe stream: %v", err)
		}

		for _, shard := range result.StreamDescription.Shards {
			s.shardChan <- shard
		}

		if result.StreamDescription.LastEvaluatedShardId == nil {
			logger.FromContext(ctx).Info("Finished reading all the shards")
			// If LastEvaluatedShardId is null, we've read all the shards.
			break
		}

		// Set up the next page query with the LastEvaluatedShardId
		exclusiveStartShardId = result.StreamDescription.LastEvaluatedShardId
	}
}
