package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSettings_Validate(t *testing.T) {
	dynamoCfg := &DynamoDB{
		OffsetFile:         "offset.txt",
		AwsRegion:          "us-east-1",
		AwsAccessKeyID:     "key-id",
		AwsSecretAccessKey: "secret-access-key",
		StreamArn:          "arn:aws:dynamodb:us-east-1:123456789012:table/TableName/stream/2020-01-01T00:00:00.000",
		TableName:          "TableName",
	}

	type _tc struct {
		name        string
		settings    *Settings
		expectedErr string
	}

	tcs := []_tc{
		{
			name:        "nil",
			expectedErr: "config is nil",
		},
		{
			name:        "nil source",
			settings:    &Settings{},
			expectedErr: "invalid source: ''",
		},
		{
			name:        "invalid source",
			settings:    &Settings{Source: "foo"},
			expectedErr: "invalid source: 'foo'",
		},
		{
			name: "nil dynamodb",
			settings: &Settings{
				Source:   SourceDynamo,
				DynamoDB: nil,
			},
			expectedErr: "dynamodb config is nil",
		},
		{
			name:        "nil destination",
			settings:    &Settings{Source: SourceDynamo, DynamoDB: dynamoCfg},
			expectedErr: "invalid destination: ''",
		},
		{
			name:        "invalid destination",
			settings:    &Settings{Source: SourceDynamo, DynamoDB: dynamoCfg, Destination: "foo"},
			expectedErr: "invalid destination: 'foo'",
		},
		{
			name:        "nil kafka",
			settings:    &Settings{Source: SourceDynamo, DynamoDB: dynamoCfg, Destination: DestinationKafka},
			expectedErr: "kafka config is nil",
		},
		{
			name: "valid",
			settings: &Settings{
				Source:      SourceDynamo,
				DynamoDB:    dynamoCfg,
				Destination: DestinationKafka,
				Kafka: &Kafka{
					BootstrapServers: "localhost:9092",
					TopicPrefix:      "test",
				},
			},
		},
	}

	for _, tc := range tcs {
		err := tc.settings.Validate()
		if tc.expectedErr != "" {
			assert.ErrorContains(t, err, tc.expectedErr, tc.name)
		} else {
			assert.NoError(t, err, tc.name)
		}

	}
}
