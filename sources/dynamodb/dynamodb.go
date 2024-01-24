package dynamodb

import (
	"context"
	"log/slog"
	"time"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/lib/s3lib"
	"github.com/artie-labs/reader/sources/dynamodb/offsets"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

type Store struct {
	s3Client       *s3lib.S3Client
	dynamoDBClient *dynamodb.DynamoDB

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
		logger.Fatal("Failed to create session", slog.Any("err", err))
	}

	store := &Store{
		tableName: cfg.DynamoDB.TableName,
		streamArn: cfg.DynamoDB.StreamArn,
		batchSize: cfg.Kafka.PublishSize,
		cfg:       cfg.DynamoDB,
	}

	if cfg.DynamoDB.Snapshot {
		// Snapshot needs the DynamoDB client to describe table and S3 library to read from the files.
		store.dynamoDBClient = dynamodb.New(sess)
		store.s3Client = s3lib.NewClient(sess)
	} else {
		// If it's not snapshotting, then we'll need to create offset storage, streams client and a channel.
		store.storage = offsets.NewStorage(ctx, cfg.DynamoDB.OffsetFile, nil, nil)
		store.streams = dynamodbstreams.New(sess)
		store.shardChan = make(chan *dynamodbstreams.Shard)
	}

	return store
}

func (s *Store) Run(ctx context.Context) {
	if s.cfg.Snapshot {
		if err := s.scanFilesOverBucket(); err != nil {
			logger.Fatal("Scanning files over bucket failed", slog.Any("err", err))
		}

		if err := s.streamAndPublish(ctx); err != nil {
			logger.Fatal("Stream and publish failed", slog.Any("err", err))
		}

		slog.Info("Finished snapshotting all the files")
	} else {
		ticker := time.NewTicker(shardScannerInterval)

		// Start to subscribe to the channel
		go s.ListenToChannel(ctx)

		// Scan it for the first time manually, so we don't have to wait 5 mins
		s.scanForNewShards(ctx)
		for {
			select {
			case <-ctx.Done():
				close(s.shardChan)
				slog.Info("Terminating process...")
				return
			case <-ticker.C:
				slog.Info("Scanning for new shards...")
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
			logger.Fatal("Failed to describe stream", slog.Any("err", err))
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
}
