package config

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/stringutil"
	"gopkg.in/yaml.v2"
	"log"
	"os"
)

const ctxKey = "_cfg"

type Kafka struct {
	BootstrapServers string `yaml:"bootstrapServers"`
	TopicPrefix      string `yaml:"topicPrefix"`
	AwsEnabled       bool   `yaml:"awsEnabled"`
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

type DynamoDB struct {
	OffsetFile         string `yaml:"offsetFile"`
	AwsRegion          string `yaml:"awsRegion"`
	AwsAccessKeyID     string `yaml:"awsAccessKeyId"`
	AwsSecretAccessKey string `yaml:"awsSecretAccessKey"`
	StreamArn          string `yaml:"streamArn"`
	TableName          string `yaml:"tableName"`
}

func (d *DynamoDB) Validate() error {
	if d == nil {
		return fmt.Errorf("dynamodb config is nil")
	}

	if stringutil.Empty(d.OffsetFile, d.AwsRegion, d.AwsAccessKeyID, d.AwsSecretAccessKey, d.StreamArn, d.TableName) {
		return fmt.Errorf("one of the dynamoDB configs is empty: offsetFile, awsRegion, awsAccessKeyID, awsSecretAccessKey, streamArn or tableName")
	}

	return nil
}

type Reporting struct {
	Sentry *Sentry `yaml:"sentry"`
}

type Sentry struct {
	DSN string `yaml:"dsn"`
}

type Settings struct {
	DynamoDB  *DynamoDB  `yaml:"dynamodb"`
	Reporting *Reporting `yaml:"reporting"`
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

func InjectIntoContext(ctx context.Context, fp string) context.Context {
	bytes, err := os.ReadFile(fp)
	if err != nil {
		log.Fatalf("failed to read config file, err: %v", err)
	}

	var settings Settings
	err = yaml.Unmarshal(bytes, &settings)
	if err != nil {
		log.Fatalf("failed to unmarshal config file, err: %v", err)
	}

	return context.WithValue(ctx, ctxKey, &settings)
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
