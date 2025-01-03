package config

import (
	"cmp"
	"fmt"
	"os"
	"strings"

	transferCfg "github.com/artie-labs/transfer/lib/config"
	"gopkg.in/yaml.v3"

	"github.com/artie-labs/reader/constants"
)

type Kafka struct {
	// Required
	BootstrapServers string `yaml:"bootstrapServers"`
	TopicPrefix      string `yaml:"topicPrefix"`
	// Optional
	AwsEnabled     bool   `yaml:"awsEnabled,omitempty"`
	PublishSize    uint   `yaml:"publishSize,omitempty"`
	MaxRequestSize uint64 `yaml:"maxRequestSize,omitempty"`
	// If username and password are passed in, we'll use SCRAM w/ SHA512.
	Username   string `yaml:"username,omitempty"`
	Password   string `yaml:"password,omitempty"`
	DisableTLS bool   `yaml:"disableTLS,omitempty"`
}

type Mechanism string

const (
	None        Mechanism = ""
	ScramSha512 Mechanism = "SCRAM-SHA-512"
	AwsMskIam   Mechanism = "AWS-MSK-IAM"
)

func (k *Kafka) Mechanism() Mechanism {
	if k.Username != "" && k.Password != "" {
		return ScramSha512
	}

	if k.AwsEnabled {
		return AwsMskIam
	}

	return None
}

func (k *Kafka) BootstrapAddresses() []string {
	return strings.Split(k.BootstrapServers, ",")
}

func (k *Kafka) GetPublishSize() uint {
	return cmp.Or(k.PublishSize, constants.DefaultPublishSize)
}

func (k *Kafka) Validate() error {
	if k == nil {
		return fmt.Errorf("kafka config is nil")
	}

	if k.BootstrapServers == "" {
		return fmt.Errorf("bootstrap servers not passed in")
	}

	if k.TopicPrefix == "" {
		return fmt.Errorf("topic prefix not passed in")
	}

	return nil
}

type Reporting struct {
	Sentry *Sentry `yaml:"sentry"`
}

type Sentry struct {
	DSN string `yaml:"dsn"`
}

type Metrics struct {
	Namespace string   `yaml:"namespace"`
	Tags      []string `yaml:"tags"`
}

type Source string

const (
	SourceDynamo     Source = "dynamodb"
	SourceMongoDB    Source = "mongodb"
	SourceMSSQL      Source = "mssql"
	SourceMySQL      Source = "mysql"
	SourcePostgreSQL Source = "postgresql"
)

type Destination string

const (
	DestinationKafka    Destination = "kafka"
	DestinationTransfer Destination = "transfer"
)

type BeforeBackfill string

const (
	BeforeBackfillDoNothing     BeforeBackfill = ""
	BeforeBackfillTruncateTable BeforeBackfill = "truncate_table"
	BeforeBackfillDropTable     BeforeBackfill = "drop_table"
)

type Settings struct {
	Source     Source      `yaml:"source"`
	DynamoDB   *DynamoDB   `yaml:"dynamodb,omitempty"`
	MongoDB    *MongoDB    `yaml:"mongodb,omitempty"`
	MySQL      *MySQL      `yaml:"mysql,omitempty"`
	PostgreSQL *PostgreSQL `yaml:"postgresql,omitempty"`
	MSSQL      *MSSQL      `yaml:"mssql,omitempty"`

	Destination Destination         `yaml:"destination"`
	Kafka       *Kafka              `yaml:"kafka,omitempty"`
	Transfer    *transferCfg.Config `yaml:"transfer,omitempty"`

	Reporting *Reporting `yaml:"reporting"`
	Metrics   *Metrics   `yaml:"metrics"`

	BeforeBackfill BeforeBackfill `yaml:"beforeBackfill,omitempty"`
}

func (s *Settings) Validate() error {
	if s == nil {
		return fmt.Errorf("config is nil")
	}

	switch s.Source {
	case SourceDynamo:
		if s.DynamoDB == nil {
			return fmt.Errorf("dynamodb config is nil")
		}

		if err := s.DynamoDB.Validate(); err != nil {
			return fmt.Errorf("dynamodb validation failed: %w", err)
		}
	case SourceMongoDB:
		if s.MongoDB == nil {
			return fmt.Errorf("mongodb config is nil")
		}

		if err := s.MongoDB.Validate(); err != nil {
			return fmt.Errorf("mongodb validation failed: %w", err)
		}
	case SourceMSSQL:
		if s.MSSQL == nil {
			return fmt.Errorf("mssql config is nil")
		}

		if err := s.MSSQL.Validate(); err != nil {
			return fmt.Errorf("mssql validation failed: %w", err)
		}
	case SourceMySQL:
		if s.MySQL == nil {
			return fmt.Errorf("mysql config is nil")
		}

		if err := s.MySQL.Validate(); err != nil {
			return fmt.Errorf("mysql validation failed: %w", err)
		}
	case SourcePostgreSQL:
		if s.PostgreSQL == nil {
			return fmt.Errorf("postgres config is nil")
		}

		if err := s.PostgreSQL.Validate(); err != nil {
			return fmt.Errorf("postgres validation failed: %w", err)
		}
	default:
		return fmt.Errorf("invalid source: %q", s.Source)
	}

	switch s.Destination {
	case DestinationKafka:
		if s.Kafka == nil {
			return fmt.Errorf("kafka config is nil")
		}

		if err := s.Kafka.Validate(); err != nil {
			return fmt.Errorf("kafka validation failed: %w", err)
		}
	case DestinationTransfer:
		if s.Transfer == nil {
			return fmt.Errorf("transfer config is nil")
		}

		topicConfigs, err := s.Transfer.TopicConfigs()
		if err != nil {
			return fmt.Errorf("transfer topic configs are invalid: %w", err)
		}
		if len(topicConfigs) != 1 {
			return fmt.Errorf("expected exactly one transfer config, got %d", len(topicConfigs))
		}

		for _, topicConfig := range topicConfigs {
			topicConfig.Load()
		}

		if err := s.Transfer.Validate(); err != nil {
			return fmt.Errorf("transfer validation failed: %w", err)
		}

		if s.Transfer.Mode != transferCfg.Replication {
			return fmt.Errorf("transfer mode must be replication")
		}
	default:
		return fmt.Errorf("invalid destination: %q", s.Destination)
	}

	return nil
}

func ReadConfig(fp string) (*Settings, error) {
	bytes, err := os.ReadFile(fp)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var settings Settings
	err = yaml.Unmarshal(bytes, &settings)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal config file: %w", err)
	}

	// By default, if you don't pass in a source -- it will be dynamodb for backwards compatibility
	settings.Source = cmp.Or(settings.Source, SourceDynamo)

	// By default, if you don't pass in a destination -- it will be Kafka for backwards compatibility
	settings.Destination = cmp.Or(settings.Destination, DestinationKafka)

	if err = settings.Validate(); err != nil {
		return nil, fmt.Errorf("failed to validate config file: %w", err)
	}

	return &settings, nil
}
