package dynamodb

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/s3lib"
	"github.com/artie-labs/reader/sources/dynamodb/offsets"
)

type Store struct {
	s3Client       *s3lib.S3Client
	dynamoDBClient *dynamodb.DynamoDB

	tableName string
	streamArn string
	streams   *dynamodbstreams.DynamoDBStreams
	storage   *offsets.OffsetStorage
	shardChan chan *dynamodbstreams.Shard

	cfg *config.DynamoDB
}

// jitterSleepBaseMs - sleep for 50 ms as the base.
const jitterSleepBaseMs = 50
const shardScannerInterval = 5 * time.Minute

func Load(cfg config.DynamoDB) (*Store, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      ptr.ToString(cfg.AwsRegion),
		Credentials: credentials.NewStaticCredentials(cfg.AwsAccessKeyID, cfg.AwsSecretAccessKey, ""),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create session, err: %w", err)
	}

	store := &Store{
		tableName: cfg.TableName,
		streamArn: cfg.StreamArn,
		cfg:       &cfg,
	}

	if cfg.Snapshot {
		// Snapshot needs the DynamoDB client to describe table and S3 library to read from the files.
		store.dynamoDBClient = dynamodb.New(sess)
		store.s3Client = s3lib.NewClient(sess)
	} else {
		// If it's not snapshotting, then we'll need to create offset storage, streams client and a channel.
		store.storage = offsets.NewStorage(cfg.OffsetFile, nil, nil)
		store.streams = dynamodbstreams.New(sess)
		store.shardChan = make(chan *dynamodbstreams.Shard)
	}

	return store, nil
}

func (s *Store) Run(ctx context.Context, writer kafkalib.BatchWriter) error {
	if s.cfg.Snapshot {
		if err := s.scanFilesOverBucket(); err != nil {
			return fmt.Errorf("scanning files over bucket failed, err: %w", err)
		}

		if err := s.streamAndPublish(ctx, writer); err != nil {
			return fmt.Errorf("stream and publish failed, err: %w", err)
		}

		slog.Info("Finished snapshotting all the files")
	} else {
		ticker := time.NewTicker(shardScannerInterval)

		// Start to subscribe to the channel
		go s.ListenToChannel(ctx, writer)

		// Scan it for the first time manually, so we don't have to wait 5 mins
		if err := s.scanForNewShards(); err != nil {
			return fmt.Errorf("failed to scan for new shards, err: %w", err)
		}
		for {
			select {
			case <-ctx.Done():
				close(s.shardChan)
				slog.Info("Terminating process...")
				return nil
			case <-ticker.C:
				slog.Info("Scanning for new shards...")
				if err := s.scanForNewShards(); err != nil {
					return fmt.Errorf("failed to scan for new shards, err: %w", err)
				}
			}
		}
	}
	return nil
}

func (s *Store) scanForNewShards() error {
	var exclusiveStartShardId *string
	for {
		input := &dynamodbstreams.DescribeStreamInput{
			StreamArn:             aws.String(s.streamArn),
			ExclusiveStartShardId: exclusiveStartShardId,
		}

		result, err := s.streams.DescribeStream(input)
		if err != nil {
			return fmt.Errorf("failed to describe stream, err: %w", err)
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
