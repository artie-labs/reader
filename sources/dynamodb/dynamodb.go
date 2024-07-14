package dynamodb

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"time"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/s3lib"
	"github.com/artie-labs/reader/sources"
	"github.com/artie-labs/reader/sources/dynamodb/offsets"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
)

// jitterSleepBaseMs - sleep for 50 ms as the base.
const jitterSleepBaseMs = 50
const shardScannerInterval = 5 * time.Minute

func Load(cfg config.DynamoDB) (sources.Source, bool, error) {
	_awsCfg, err := awsCfg.LoadDefaultConfig(context.TODO(),
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
			s3Client:       s3lib.NewClient(_awsCfg),
		}, false, nil
	} else {
		return &StreamStore{
			tableName: cfg.TableName,
			streamArn: cfg.StreamArn,
			cfg:       &cfg,
			storage:   offsets.NewStorage(cfg.OffsetFile, nil, nil),
			streams:   dynamodbstreams.NewFromConfig(_awsCfg),
			shardChan: make(chan *types.Shard),
		}, true, nil
	}
}
