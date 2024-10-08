package config

import (
	"cmp"
	"fmt"
	"github.com/artie-labs/reader/constants"

	"github.com/artie-labs/transfer/lib/stringutil"

	"github.com/artie-labs/reader/lib/s3lib"
)

type DynamoDB struct {
	OffsetFile         string            `yaml:"offsetFile"`
	AwsAccessKeyID     string            `yaml:"awsAccessKeyId"`
	AwsSecretAccessKey string            `yaml:"awsSecretAccessKey"`
	StreamArn          string            `yaml:"streamArn"`
	TableName          string            `yaml:"tableName"`
	Snapshot           bool              `yaml:"snapshot"`
	SnapshotSettings   *SnapshotSettings `yaml:"snapshotSettings"`
}

func (d *DynamoDB) Validate() error {
	if d == nil {
		return fmt.Errorf("dynamodb config is nil")
	}

	if stringutil.Empty(d.OffsetFile, d.AwsAccessKeyID, d.AwsSecretAccessKey, d.StreamArn, d.TableName) {
		return fmt.Errorf("one of the dynamoDB configs is empty: offsetFile, awsAccessKeyID, awsSecretAccessKey, streamArn or tableName")
	}

	if d.Snapshot {
		if err := d.SnapshotSettings.Validate(); err != nil {
			return fmt.Errorf("snapshot validation failed: %w", err)
		}
	}

	return nil
}

type SnapshotSettings struct {
	ShouldInitiateExport bool           `yaml:"shouldInitiateExport"` // Whether Reader should initiate a DDB export or not.
	Folder               string         `yaml:"folder"`               // The folder where the snapshot files will be stored.
	SpecifiedFiles       []s3lib.S3File `yaml:"specifiedFiles"`       // If this is passed in, we'll only process these files
	BatchSize            int32          `yaml:"batchSize"`
}

func (s *SnapshotSettings) GetBatchSize() int32 {
	return cmp.Or(s.BatchSize, constants.DefaultBatchSize)
}

func (s *SnapshotSettings) Validate() error {
	if s == nil {
		return fmt.Errorf("settings is nil")
	}

	if s.Folder == "" {
		return fmt.Errorf("folder is empty")
	}

	return nil
}
