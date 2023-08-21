package config

import (
	"context"
	"gopkg.in/yaml.v2"
	"log"
	"os"
)

const ctxKey = "_cfg"

type Kafka struct {
	BootstrapServers string `yaml:"bootstrapServers"`
	TopicPrefix      string `yaml:"topic"`
	AwsEnabled       bool   `yaml:"awsEnabled"`
}

type DynamoDB struct {
	OffsetFile         string `yaml:"offsetFile"`
	AwsRegion          string `yaml:"awsRegion"`
	AwsAccessKeyID     string `yaml:"awsAccessKeyId"`
	AwsSecretAccessKey string `yaml:"awsSecretAccessKey"`
	StreamArn          string `yaml:"streamArn"`
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
