package dynamodb

import (
	"cmp"
	"context"
	"fmt"
	"time"

	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/s3lib"
	"github.com/artie-labs/reader/lib/throttler"
	"github.com/artie-labs/reader/sources"
	"github.com/artie-labs/reader/sources/dynamodb/offsets"
)

const (
	jitterSleepBaseMs             = 100
	shardScannerInterval          = 5 * time.Minute
	defaultConcurrencyLimit int64 = 100
)

func Load(ctx context.Context, cfg config.DynamoDB) (sources.Source, bool, error) {
	// TODO: Parse `arn` to get the region.
	_awsCfg, err := awsCfg.LoadDefaultConfig(ctx,
		awsCfg.WithRegion(cfg.AwsRegion),
		awsCfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AwsAccessKeyID, cfg.AwsSecretAccessKey, "")),
	)

	if err != nil {
		return nil, false, fmt.Errorf("failed to load AWS config: %w", err)
	}

	if cfg.Snapshot {
		return &SnapshotStore{
			tableName:      cfg.TableName,
			streamArn:      cfg.StreamArn,
			cfg:            &cfg,
			dynamoDBClient: dynamodb.NewFromConfig(_awsCfg),
			s3Client:       s3lib.NewClient(cfg.SnapshotSettings.S3Bucket, _awsCfg),
		}, false, nil
	} else {
		// TODO: Should we be throttling based on go-routines? Or should we be using buffered channels?
		_throttler, err := throttler.NewThrottler(cmp.Or(cfg.MaxConcurrency, defaultConcurrencyLimit))
		if err != nil {
			return nil, false, fmt.Errorf("failed to create throttler: %w", err)
		}

		return &StreamStore{
			tableName: cfg.TableName,
			streamArn: cfg.StreamArn,
			cfg:       &cfg,
			storage:   offsets.NewStorage(cfg.OffsetFile, nil, nil),
			streams:   dynamodbstreams.NewFromConfig(_awsCfg),
			shardChan: make(chan types.Shard),
			throttler: _throttler,
		}, true, nil
	}
}
