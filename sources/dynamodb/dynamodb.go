package dynamodb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/sources"
	"github.com/artie-labs/reader/sources/dynamodb/snapshot"
	"github.com/artie-labs/reader/sources/dynamodb/stream"
)

func Load(ctx context.Context, cfg config.DynamoDB) (sources.Source, bool, error) {
	parsedArn, err := arn.Parse(cfg.StreamArn)
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse stream ARN: %w", err)
	}

	_awsCfg, err := awsCfg.LoadDefaultConfig(ctx,
		awsCfg.WithRegion(parsedArn.Region),
		awsCfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AwsAccessKeyID, cfg.AwsSecretAccessKey, "")),
	)

	if err != nil {
		return nil, false, fmt.Errorf("failed to load AWS config: %w", err)
	}

	if cfg.Snapshot {
		store, err := snapshot.NewStore(cfg, _awsCfg)
		if err != nil {
			return nil, false, err
		}

		return store, false, nil
	} else {
		return stream.NewStore(cfg, _awsCfg), true, nil
	}
}
