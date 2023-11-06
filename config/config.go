package config

import (
	"context"
	"fmt"
	"gopkg.in/yaml.v2"
	"log"
	"os"
)

const ctxKey = "_cfg"

type Kafka struct {
	BootstrapServers string `yaml:"bootstrapServers"`
	TopicPrefix      string `yaml:"topicPrefix"`
	AwsEnabled       bool   `yaml:"awsEnabled"`
	PublishSize      int    `yaml:"publishSize"`
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

type Settings struct {
	DynamoDB  *DynamoDB  `yaml:"dynamodb"`
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

	if s.DynamoDB == nil {
		return fmt.Errorf("dynamodb config is nil")
	}

	if err := s.DynamoDB.Validate(); err != nil {
		return fmt.Errorf("dynamodb validation failed: %v", err)
	}

	return nil
}

func ReadConfig(fp string) (*Settings, error) {
	bytes, err := os.ReadFile(fp)
	if err != nil {
		log.Fatalf("failed to read config file, err: %v", err)
	}

	var settings Settings
	err = yaml.Unmarshal(bytes, &settings)
	if err != nil {
		log.Fatalf("failed to unmarshal config file, err: %v", err)
	}

	if err = settings.Validate(); err != nil {
		log.Fatalf("failed to validate config file, err: %v", err)
	}

	settings.Kafka.GenerateDefault()
	return &settings, nil
}

func InjectIntoContext(ctx context.Context, settings *Settings) context.Context {
	return context.WithValue(ctx, ctxKey, settings)
}

func FromContext(ctx context.Context) *Settings {
	val := ctx.Value(ctxKey)
	if val == nil {
		return nil
	}

	settings, isOk := val.(*Settings)
	if !isOk {
		return nil
	}

	return settings
}
