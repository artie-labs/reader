package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/artie-labs/reader/constants"
)

type Kafka struct {
	BootstrapServers string `yaml:"bootstrapServers"`
	TopicPrefix      string `yaml:"topicPrefix"`
	AwsEnabled       bool   `yaml:"awsEnabled"`
	PublishSize      uint   `yaml:"publishSize,omitempty"`
	MaxRequestSize   uint64 `yaml:"maxRequestSize,omitempty"`
}

func (k *Kafka) BootstrapAddresses() []string {
	return strings.Split(k.BootstrapServers, ",")
}

func (k *Kafka) GetPublishSize() uint {
	if k.PublishSize == 0 {
		return constants.DefaultPublishSize
	}

	return k.PublishSize
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
	SourcePostgreSQL Source = "postgresql"
)

type Settings struct {
	Source     Source      `yaml:"source"`
	PostgreSQL *PostgreSQL `yaml:"postgresql,omitempty"`
	DynamoDB   *DynamoDB   `yaml:"dynamodb,omitempty"`

	Reporting *Reporting `yaml:"reporting"`
	Metrics   *Metrics   `yaml:"metrics"`
	Kafka     *Kafka     `yaml:"kafka"`
}

func (s *Settings) Validate() error {
	if s == nil {
		return fmt.Errorf("config is nil")
	}

	if s.Kafka == nil {
		return fmt.Errorf("kafka config is nil")
	}

	if err := s.Kafka.Validate(); err != nil {
		return fmt.Errorf("kafka validation failed: %w", err)
	}

	switch s.Source {
	// By default, if you don't pass in a source -- it will be dynamodb for backwards compatibility
	case SourceDynamo, "":
		if s.DynamoDB == nil {
			return fmt.Errorf("dynamodb config is nil")
		}

		if err := s.DynamoDB.Validate(); err != nil {
			return fmt.Errorf("dynamodb validation failed: %w", err)
		}
	case SourcePostgreSQL:
		if s.PostgreSQL == nil {
			return fmt.Errorf("postgres config is nil")
		}

		if err := s.PostgreSQL.Validate(); err != nil {
			return fmt.Errorf("postgres validation failed: %w", err)
		}
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

	if err = settings.Validate(); err != nil {
		return nil, fmt.Errorf("failed to validate config file: %w", err)
	}

	return &settings, nil
}
