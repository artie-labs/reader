package config

import (
	"context"
	"fmt"
	"log"
	"os"

	"gopkg.in/yaml.v2"

	"github.com/artie-labs/reader/constants"
)

type Kafka struct {
	BootstrapServers string `yaml:"bootstrapServers"`
	TopicPrefix      string `yaml:"topicPrefix"`
	AwsEnabled       bool   `yaml:"awsEnabled"`
	PublishSize      uint   `yaml:"publishSize,omitempty"`
	MaxRequestSize   uint64 `yaml:"maxRequestSize,omitempty"`
}

func (k *Kafka) GenerateDefault() {
	if k.PublishSize == 0 {
		k.PublishSize = 2500
	}
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
	PostgreSQL *PostgreSQL `yaml:"postgresql"`
	DynamoDB   *DynamoDB   `yaml:"dynamodb"`

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
		return fmt.Errorf("kafka validation failed: %v", err)
	}

	switch s.Source {
	case SourceDynamo, "":
		if s.DynamoDB == nil {
			return fmt.Errorf("dynamodb config is nil")
		}

		if err := s.DynamoDB.Validate(); err != nil {
			return fmt.Errorf("dynamodb validation failed: %v", err)
		}
	case SourcePostgreSQL:
		if s.PostgreSQL == nil {
			return fmt.Errorf("postgres config is nil")
		}
	}

	return nil
}

func ReadConfig(fp string) (*Settings, error) {
	bytes, err := os.ReadFile(fp)
	if err != nil {
		log.Fatalf("Failed to read config file, err: %v", err)
	}

	var settings Settings
	err = yaml.Unmarshal(bytes, &settings)
	if err != nil {
		log.Fatalf("Failed to unmarshal config file, err: %v", err)
	}

	if err = settings.Validate(); err != nil {
		log.Fatalf("Failed to validate config file, err: %v", err)
	}

	settings.Kafka.GenerateDefault()
	return &settings, nil
}

func InjectIntoContext(ctx context.Context, settings *Settings) context.Context {
	return context.WithValue(ctx, constants.ConfigKey, settings)
}

func FromContext(ctx context.Context) *Settings {
	val := ctx.Value(constants.ConfigKey)
	if val == nil {
		return nil
	}

	settings, isOk := val.(*Settings)
	if !isOk {
		return nil
	}

	return settings
}
