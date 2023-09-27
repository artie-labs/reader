package dynamodb

import (
	"context"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/lib/s3lib"
	"github.com/artie-labs/reader/sources/dynamodb/offsets"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"time"
)

type Store struct {
	s3Client *s3lib.S3Client

	tableName string
	streamArn string
	batchSize int
	streams   *dynamodbstreams.DynamoDBStreams
	storage   *offsets.OffsetStorage
	shardChan chan *dynamodbstreams.Shard

	cfg *config.DynamoDB
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

	s3Client, err := s3lib.NewClient(cfg.DynamoDB.AwsRegion)
	if err != nil {
		logger.FromContext(ctx).Fatalf("Failed to create s3 client: %v", err)
	}

	store := &Store{
		s3Client:  s3Client,
		tableName: cfg.DynamoDB.TableName,
		streamArn: cfg.DynamoDB.StreamArn,
		batchSize: cfg.Kafka.PublishSize,
		streams:   dynamodbstreams.New(sess),
		shardChan: make(chan *dynamodbstreams.Shard),
		cfg:       cfg.DynamoDB,
	}

	// Snapshot mode does not need to use storage
	if !cfg.DynamoDB.Snapshot {
		store.storage = offsets.NewStorage(ctx, cfg.DynamoDB.OffsetFile, nil, nil)

	}

	return store
}

func (s *Store) Run(ctx context.Context) {
	if s.cfg.Snapshot {
		if err := s.scanFilesOverBucket(); err != nil {
			logger.FromContext(ctx).WithError(err).Fatalf("scanning files over bucket failed")
		}

		if err := s.ReadAndPublish(ctx); err != nil {
			logger.FromContext(ctx).WithError(err).Fatalf("scanning files over bucket failed")
		}

		logger.FromContext(ctx).Info("Finished snapshotting all the files")
	} else {
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
