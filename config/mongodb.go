package config

import (
	"cmp"
	"fmt"

	"github.com/artie-labs/reader/constants"
	"github.com/artie-labs/transfer/lib/stringutil"
)

func (s StreamingSettings) Validate() error {
	if !s.Enabled {
		return nil
	}

	if s.OffsetFile == "" {
		return fmt.Errorf("offset file must be passed in when streaming is enabled")
	}

	return nil
}

type StreamingSettings struct {
	Enabled    bool   `yaml:"enabled,omitempty"`
	OffsetFile string `yaml:"offsetFile,omitempty"`
	BatchSize  int32  `yaml:"batchSize,omitempty"`
}

type MongoDB struct {
	Host              string            `yaml:"host"`
	Username          string            `yaml:"username"`
	Password          string            `yaml:"password"`
	Database          string            `yaml:"database"`
	Collections       []Collection      `yaml:"collections"`
	StreamingSettings StreamingSettings `yaml:"streamingSettings,omitempty"`
	DisableTLS        bool              `yaml:"disableTLS"`
}

type Collection struct {
	Name          string `yaml:"name"`
	BatchSize     int32  `yaml:"batchSize,omitempty"`
	StartObjectID string `yaml:"startObjectID,omitempty"`
	EndObjectID   string `yaml:"endObjectID,omitempty"`
}

func (c Collection) TopicSuffix(db string) string {
	return fmt.Sprintf("%s.%s", db, c.Name)
}

func (c Collection) GetBatchSize() int32 {
	return cmp.Or(c.BatchSize, constants.DefaultBatchSize)
}

func (m MongoDB) GetStreamingBatchSize() int32 {
	return cmp.Or(m.StreamingSettings.BatchSize, constants.DefaultBatchSize)
}

func (m MongoDB) Validate() error {
	if stringutil.Empty(m.Host, m.Database, m.Username, m.Password) {
		return fmt.Errorf("one of the MongoDB settings is empty: host, username, password, database")
	}

	if len(m.Collections) == 0 {
		return fmt.Errorf("no collections passed in")
	}

	for _, collection := range m.Collections {
		if collection.Name == "" {
			return fmt.Errorf("collection name must be passed in")
		}
	}

	return m.StreamingSettings.Validate()
}
