package dynamodb

import (
	"context"
	"github.com/artie-labs/reader/config"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLoad(t *testing.T) {
	cfg := config.DynamoDB{
		OffsetFile:         "",
		AwsAccessKeyID:     "",
		AwsSecretAccessKey: "",
		StreamArn:          "arn:aws:dynamodb:us-east-1:99999:table/ddb-test/stream/2024-04-26T00:54:24.794",
		TableName:          "",
		MaxConcurrency:     0,
		Snapshot:           false,
		SnapshotSettings:   nil,
	}

	_, _, err := Load(context.Background(), cfg)
	assert.NoError(t, err)

	parsedArn, err := arn.Parse(cfg.StreamArn)
	assert.NoError(t, err)
	assert.Equal(t, "us-east-1", parsedArn.Region)
}
