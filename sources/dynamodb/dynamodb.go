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
	"github.com/artie-labs/reader/sources"
	"github.com/artie-labs/reader/sources/dynamodb/offsets"
)

// jitterSleepBaseMs - sleep for 50 ms as the base.
const jitterSleepBaseMs = 50
const shardScannerInterval = 5 * time.Minute

func Load(cfg config.DynamoDB) (sources.Source, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      ptr.ToString(cfg.AwsRegion),
		Credentials: credentials.NewStaticCredentials(cfg.AwsAccessKeyID, cfg.AwsSecretAccessKey, ""),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	if cfg.Snapshot {
		store := &SnapshotStore{
			tableName: cfg.TableName,
			streamArn: cfg.StreamArn,
			cfg:       &cfg,
		}

		// Snapshot needs the DynamoDB client to describe table and S3 library to read from the files.
		store.dynamoDBClient = dynamodb.New(sess)
		store.s3Client = s3lib.NewClient(sess)
		return store, nil
	} else {
		store := &StreamStore{
			tableName: cfg.TableName,
			streamArn: cfg.StreamArn,
			cfg:       &cfg,
		}

		// If it's not snapshotting, then we'll need to create offset storage, streams client and a channel.
		store.storage = offsets.NewStorage(cfg.OffsetFile, nil, nil)
		store.streams = dynamodbstreams.New(sess)
		store.shardChan = make(chan *dynamodbstreams.Shard)
		return store, nil
	}
}
