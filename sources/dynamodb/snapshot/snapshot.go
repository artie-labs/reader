package snapshot

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/dynamo"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/lib/s3lib"
	"github.com/artie-labs/reader/writers"
)

type Store struct {
	tableName    string
	streamArn    string
	s3BucketName string
	s3PrefixName string

	cfg            *config.DynamoDB
	s3Client       *s3lib.S3Client
	dynamoDBClient *dynamodb.Client
}

func NewStore(cfg config.DynamoDB, awsCfg aws.Config) (*Store, error) {
	bucketName, prefixName, err := s3lib.BucketAndPrefixFromFilePath(cfg.SnapshotSettings.Folder)
	if err != nil {
		return nil, err
	}

	return &Store{
		tableName:      cfg.TableName,
		streamArn:      cfg.StreamArn,
		s3BucketName:   bucketName,
		s3PrefixName:   prefixName,
		cfg:            &cfg,
		s3Client:       s3lib.NewClient(bucketName, awsCfg),
		dynamoDBClient: dynamodb.NewFromConfig(awsCfg),
	}, nil
}

func (s *Store) Close() error {
	return nil
}

func (s *Store) Run(ctx context.Context, writer writers.Writer) error {
	start := time.Now()
	if err := s.scanFilesOverBucket(ctx); err != nil {
		return fmt.Errorf("scanning files over bucket failed: %w", err)
	}

	keys, err := dynamo.RetrievePrimaryKeys(ctx, s.dynamoDBClient, s.tableName)
	if err != nil {
		return fmt.Errorf("failed to retrieve primary keys: %w", err)
	}

	ch := make(chan map[string]types.AttributeValue)
	go func() {
		if err = s.s3Client.StreamJsonGzipFiles(ctx, s.cfg.SnapshotSettings.SpecifiedFiles, ch); err != nil {
			logger.Panic("Failed to read file", slog.Any("err", err))
		}
	}()

	count, err := writer.Write(ctx, NewSnapshotIterator(ch, keys, s.tableName, s.cfg.SnapshotSettings.GetBatchSize()))
	if err != nil {
		return fmt.Errorf("failed to snapshot: %w", err)
	}

	slog.Info("Finished snapshotting",
		slog.String("tableName", s.tableName),
		slog.Int("scannedTotal", count),
		slog.Duration("totalDuration", time.Since(start)),
	)
	return nil
}

func (s *Store) scanFilesOverBucket(ctx context.Context) error {
	if len(s.cfg.SnapshotSettings.SpecifiedFiles) > 0 {
		// Don't scan because you are already specifying files
		return nil
	}

	files, err := s.s3Client.ListFiles(ctx, s.cfg.SnapshotSettings.Folder)
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	if len(files) == 0 {
		return fmt.Errorf("no files found in the folder %q", s.cfg.SnapshotSettings.Folder)
	}

	for _, file := range files {
		slog.Info("Discovered file, adding to the processing queue...", slog.String("fileName", *file.Key))
	}

	s.cfg.SnapshotSettings.SpecifiedFiles = files
	return nil
}
