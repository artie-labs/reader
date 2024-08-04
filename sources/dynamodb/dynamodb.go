package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/ptr"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	credentialsV2 "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/s3lib"
	"github.com/artie-labs/reader/lib/throttler"
	"github.com/artie-labs/reader/sources"
	"github.com/artie-labs/reader/sources/dynamodb/offsets"
)

const (
	jitterSleepBaseMs    = 100
	shardScannerInterval = 5 * time.Minute
	// concurrencyLimit is the maximum number of shards we should be processing at once
	concurrencyLimit = 30
)

func Load(cfg config.DynamoDB) (sources.Source, bool, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      ptr.ToString(cfg.AwsRegion),
		Credentials: credentials.NewStaticCredentials(cfg.AwsAccessKeyID, cfg.AwsSecretAccessKey, ""),
	})
	if err != nil {
		return nil, false, fmt.Errorf("failed to create session: %w", err)
	}

	if cfg.Snapshot {
		_awsCfg, err := awsCfg.LoadDefaultConfig(context.Background(),
			awsCfg.WithRegion(cfg.AwsRegion),
			awsCfg.WithCredentialsProvider(credentialsV2.NewStaticCredentialsProvider(cfg.AwsAccessKeyID, cfg.AwsSecretAccessKey, "")),
		)
		if err != nil {
			return nil, false, fmt.Errorf("failed to create session v2: %w", err)
		}

		return &SnapshotStore{
			tableName:      cfg.TableName,
			streamArn:      cfg.StreamArn,
			cfg:            &cfg,
			dynamoDBClient: dynamodb.New(sess),
			s3Client:       s3lib.NewClient(cfg.SnapshotSettings.S3Bucket, _awsCfg),
		}, false, nil
	} else {
		_throttler, err := throttler.NewThrottler(concurrencyLimit)
		if err != nil {
			return nil, false, fmt.Errorf("failed to create throttler: %w", err)
		}

		return &StreamStore{
			tableName: cfg.TableName,
			streamArn: cfg.StreamArn,
			cfg:       &cfg,
			storage:   offsets.NewStorage(cfg.OffsetFile, nil, nil),
			streams:   dynamodbstreams.New(sess),
			shardChan: make(chan *dynamodbstreams.Shard),
			throttler: _throttler,
		}, true, nil
	}
}
