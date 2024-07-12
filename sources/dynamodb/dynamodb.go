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
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/reader/lib/s3lib"
	"github.com/artie-labs/reader/sources"
	"github.com/artie-labs/reader/sources/dynamodb/offsets"
)

const (
	// jitterSleepBaseMs - sleep for 50 ms as the base.
	jitterSleepBaseMs    = 50
	shardScannerInterval = 5 * time.Minute
)

func Load(cfg config.DynamoDB, statsD mtr.Client) (sources.Source, bool, error) {
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
			statsD:    statsD,
		}, true, nil
	}
}
