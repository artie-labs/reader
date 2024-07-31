package dynamodb

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/ptr"
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

const jitterSleepBaseMs = 100
const shardScannerInterval = 5 * time.Minute

// concurrencyLimit is the maximum number of shards we should be processing at once
const concurrencyLimit = 20

func Load(cfg config.DynamoDB) (sources.Source, bool, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      ptr.ToString(cfg.AwsRegion),
		Credentials: credentials.NewStaticCredentials(cfg.AwsAccessKeyID, cfg.AwsSecretAccessKey, ""),
	})
	if err != nil {
		return nil, false, fmt.Errorf("failed to create session: %w", err)
	}

	if cfg.Snapshot {
		return &SnapshotStore{
			tableName:      cfg.TableName,
			streamArn:      cfg.StreamArn,
			cfg:            &cfg,
			dynamoDBClient: dynamodb.New(sess),
			s3Client:       s3lib.NewClient(sess),
		}, false, nil
	} else {
		return &StreamStore{
			tableName: cfg.TableName,
			streamArn: cfg.StreamArn,
			cfg:       &cfg,
			storage:   offsets.NewStorage(cfg.OffsetFile, nil, nil),
			streams:   dynamodbstreams.New(sess),
			shardChan: make(chan *dynamodbstreams.Shard),
			throttler: &throttler.Throttler{Limit: concurrencyLimit},
		}, true, nil
	}
}
