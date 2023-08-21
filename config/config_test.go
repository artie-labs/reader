package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSettings_Validate(t *testing.T) {
	type _tc struct {
		name      string
		settings  *Settings
		expectErr bool
	}

	tcs := []_tc{
		{
			name:      "nil",
			expectErr: true,
		},
		{
			name: "nil kafka",
			settings: &Settings{
				Kafka: nil,
			},
			expectErr: true,
		},
		{
			name: "nil dynamodb",
			settings: &Settings{
				Kafka: &Kafka{
					BootstrapServers: "localhost:9092",
					TopicPrefix:      "test",
				},
				DynamoDB: nil,
			},
			expectErr: true,
		},
		{
			name: "valid",
			settings: &Settings{
				Kafka: &Kafka{
					BootstrapServers: "localhost:9092",
					TopicPrefix:      "test",
				},
				DynamoDB: &DynamoDB{
					OffsetFile:         "offset.txt",
					AwsRegion:          "us-east-1",
					AwsAccessKeyID:     "key-id",
					AwsSecretAccessKey: "secret-access-key",
					StreamArn:          "arn:aws:dynamodb:us-east-1:123456789012:table/TableName/stream/2020-01-01T00:00:00.000",
					TableName:          "TableName",
				},
			},
		},
	}

	for _, tc := range tcs {
		err := tc.settings.Validate()
		if tc.expectErr {
			assert.Error(t, err, tc.name)
		} else {
			assert.NoError(t, err, tc.name)
		}

	}
}
