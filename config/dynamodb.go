package config

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/stringutil"
)

type DynamoDB struct {
	OffsetFile         string `yaml:"offsetFile"`
	AwsRegion          string `yaml:"awsRegion"`
	AwsAccessKeyID     string `yaml:"awsAccessKeyId"`
	AwsSecretAccessKey string `yaml:"awsSecretAccessKey"`
	StreamArn          string `yaml:"streamArn"`
	TableName          string `yaml:"tableName"`

	Snapshot         bool              `yaml:"snapshot"`
	SnapshotSettings *SnapshotSettings `yaml:"snapshotSettings"`
}

func (d *DynamoDB) Validate() error {
	if d == nil {
		return fmt.Errorf("dynamodb config is nil")
	}

	if stringutil.Empty(d.OffsetFile, d.AwsRegion, d.AwsAccessKeyID, d.AwsSecretAccessKey, d.StreamArn, d.TableName) {
		return fmt.Errorf("one of the dynamoDB configs is empty: offsetFile, awsRegion, awsAccessKeyID, awsSecretAccessKey, streamArn or tableName")
	}

	if d.Snapshot {
		if err := d.SnapshotSettings.Validate(); err != nil {
			return fmt.Errorf("snapshot validation failed - err: %v", err)
		}
	}

	return nil
}

type SnapshotSettings struct {
	Folder string `yaml:"folder"`
	// If the files are not specified, that's okay.
	// We will scan the folder and then load into `specifiedFiles`
	SpecifiedFiles []string `yaml:"specifiedFiles"`
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
