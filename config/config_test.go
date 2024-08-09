package config

import (
	"os"
	"path/filepath"
	"testing"

	transferCfg "github.com/artie-labs/transfer/lib/config"
	transferConstants "github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func dynamoDBCfg() *DynamoDB {
	return &DynamoDB{
		OffsetFile:         "offset.txt",
		AwsAccessKeyID:     "key-id",
		AwsSecretAccessKey: "secret-access-key",
		StreamArn:          "arn:aws:dynamodb:us-east-1:123456789012:table/TableName/stream/2020-01-01T00:00:00.000",
		TableName:          "TableName",
	}
}

func TestSettings_Validate(t *testing.T) {
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
			expectedErr: `invalid source: ""`,
		},
		{
			name:        "invalid source",
			settings:    &Settings{Source: "foo"},
			expectedErr: `invalid source: "foo"`,
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
			settings:    &Settings{Source: SourceDynamo, DynamoDB: dynamoDBCfg()},
			expectedErr: `invalid destination: ""`,
		},
		{
			name:        "invalid destination",
			settings:    &Settings{Source: SourceDynamo, DynamoDB: dynamoDBCfg(), Destination: "foo"},
			expectedErr: `invalid destination: "foo"`,
		},
		{
			name:        "nil kafka",
			settings:    &Settings{Source: SourceDynamo, DynamoDB: dynamoDBCfg(), Destination: DestinationKafka},
			expectedErr: "kafka config is nil",
		},
		{
			name: "valid kafka destination",
			settings: &Settings{
				Source:      SourceDynamo,
				DynamoDB:    dynamoDBCfg(),
				Destination: DestinationKafka,
				Kafka: &Kafka{
					BootstrapServers: "localhost:9092",
					TopicPrefix:      "test",
				},
			},
		},
		{
			name:        "nil transfer",
			settings:    &Settings{Source: SourceDynamo, DynamoDB: dynamoDBCfg(), Destination: DestinationTransfer},
			expectedErr: "transfer config is nil",
		},
		{
			name: "invalid transfer destination",
			settings: &Settings{
				Source:      SourceDynamo,
				DynamoDB:    dynamoDBCfg(),
				Destination: DestinationTransfer,
				Transfer:    &transferCfg.Config{},
			},
			expectedErr: "transfer topic configs are invalid: unsupported queue",
		},
		{
			name: "valid transfer destination",
			settings: &Settings{
				Source:      SourceDynamo,
				DynamoDB:    dynamoDBCfg(),
				Destination: DestinationTransfer,
				Transfer: &transferCfg.Config{
					Mode:                 transferCfg.Replication,
					Queue:                transferConstants.Kafka,
					FlushIntervalSeconds: 10,
					FlushSizeKb:          1,
					BufferRows:           25_000,
					Kafka: &transferCfg.Kafka{
						BootstrapServer: "not-used",
						GroupID:         "group-id",
						TopicConfigs: []*kafkalib.TopicConfig{
							{
								Database:     "db",
								Schema:       "schema",
								Topic:        "unused",
								CDCFormat:    "unused",
								CDCKeyFormat: kafkalib.JSONKeyFmt,
							},
						},
					},
					Output: transferConstants.Snowflake,
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

func TestReadConfig(t *testing.T) {
	{
		// Missing file
		_, err := ReadConfig(filepath.Join(t.TempDir(), "foo.yaml"))
		assert.ErrorContains(t, err, "foo.yaml: no such file or directory")
	}
	{
		// Malformed file
		filename := filepath.Join(t.TempDir(), "foo.yaml")
		assert.NoError(t, os.WriteFile(filename, []byte("hello"), os.ModePerm))
		_, err := ReadConfig(filename)
		assert.ErrorContains(t, err, "failed to unmarshal config file")
	}
	{
		// Well-formed empty file
		filename := filepath.Join(t.TempDir(), "foo.yaml")
		assert.NoError(t, os.WriteFile(filename, []byte("{}"), os.ModePerm))
		_, err := ReadConfig(filename)
		assert.ErrorContains(t, err, "dynamodb config is nil")
	}
	{
		// Well-formed file with source and destination omitted
		filename := filepath.Join(t.TempDir(), "foo.yaml")
		settings := Settings{
			DynamoDB: dynamoDBCfg(),
			Kafka: &Kafka{
				BootstrapServers: "asdf",
				TopicPrefix:      "prefix",
			},
		}
		bytes, err := yaml.Marshal(settings)
		assert.NoError(t, err)
		assert.NoError(t, os.WriteFile(filename, bytes, os.ModePerm))
		settingsOut, err := ReadConfig(filename)
		assert.NoError(t, err)
		assert.Equal(t, settingsOut.Source, SourceDynamo)
		assert.Equal(t, settingsOut.Destination, DestinationKafka)
	}
	{
		// Well-formed file with explicit source and destination
		filename := filepath.Join(t.TempDir(), "foo.yaml")
		settings := Settings{
			Source: SourcePostgreSQL,
			PostgreSQL: &PostgreSQL{
				Host:     "host",
				Port:     5432,
				Username: "user",
				Password: "pass",
				Database: "database",
				Tables: []*PostgreSQLTable{
					{Name: "table", Schema: "schema"},
				},
			},
			Destination: DestinationKafka,
			Kafka: &Kafka{
				BootstrapServers: "asdf",
				TopicPrefix:      "prefix",
			},
		}
		bytes, err := yaml.Marshal(settings)
		assert.NoError(t, err)
		assert.NoError(t, os.WriteFile(filename, bytes, os.ModePerm))
		settingsOut, err := ReadConfig(filename)
		assert.NoError(t, err)
		assert.Equal(t, settingsOut.Source, SourcePostgreSQL)
		assert.Equal(t, settingsOut.Destination, DestinationKafka)
	}
}
