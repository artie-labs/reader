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
	"sync"
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
	}

	return store
}

func (s *Store) Run(ctx context.Context) {
	log := logger.FromContext(ctx)
	for {
		input := &dynamodbstreams.DescribeStreamInput{StreamArn: aws.String(s.streamArn)}
		result, err := s.streams.DescribeStream(input)
		if err != nil {
			log.Fatalf("Failed to describe stream: %v", err)
		}

		var wg sync.WaitGroup
		for _, shard := range result.StreamDescription.Shards {
			wg.Add(1)
			go func(shard *dynamodbstreams.Shard) {
				defer wg.Done()
				s.ProcessShard(ctx, shard)
			}(shard)
		}

		wg.Wait()
	}
}
