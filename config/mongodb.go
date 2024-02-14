package config

import (
	"fmt"
	"github.com/artie-labs/reader/constants"
	"github.com/artie-labs/transfer/lib/stringutil"
)

type MongoDB struct {
	Host     string `yaml:"host"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`

	Collections []Collection `yaml:"collections"`
}

type Collection struct {
	Name      string `yaml:"name"`
	BatchSize int32  `yaml:"batchSize,omitempty"`
	// TODO: In the future, we should be able to support customers passing Start/End PK values.
}

func (c Collection) TopicSuffix(db string) string {
	return fmt.Sprintf("%s.%s", db, c.Name)
}

func (c Collection) GetBatchSize() int32 {
	if c.BatchSize == 0 {
		return constants.DefaultBatchSize
	}

	return c.BatchSize
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

	return nil
}
