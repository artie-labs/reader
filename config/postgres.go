package config

import (
	"fmt"
	"math"

	"github.com/artie-labs/transfer/lib/stringutil"

	"github.com/artie-labs/reader/constants"
)

type PostgreSQL struct {
	Host       string             `yaml:"host"`
	Port       int                `yaml:"port"`
	Username   string             `yaml:"username"`
	Password   string             `yaml:"password"`
	Database   string             `yaml:"database"`
	Tables     []*PostgreSQLTable `yaml:"tables"`
	DisableSSL bool               `yaml:"disableSSL"`
	// Deprecated
	LegacyUsername string `yaml:"userName"`
}

func (p *PostgreSQL) GetUsername() string {
	return stringutil.Override(p.LegacyUsername, p.Username)
}

type PostgreSQLTable struct {
	Name                       string `yaml:"name"`
	Schema                     string `yaml:"schema"`
	BatchSize                  uint   `yaml:"batchSize"`
	OptionalPrimaryKeyValStart string `yaml:"optionalPrimaryKeyValStart"`
	OptionalPrimaryKeyValEnd   string `yaml:"optionalPrimaryKeyValEnd"`
}

func (p *PostgreSQLTable) GetBatchSize() uint {
	if p.BatchSize > 0 {
		return p.BatchSize
	} else {
		return constants.DefaultBatchSize
	}
}

func (p *PostgreSQL) Validate() error {
	if p == nil {
		return fmt.Errorf("the PostgreSQL config is nil")
	}

	if stringutil.Empty(p.Host, p.GetUsername(), p.Password, p.Database) {
		return fmt.Errorf("one of the PostgreSQL settings is empty: host, username, password, database")
	}

	if p.Port <= 0 {
		return fmt.Errorf("port is not set or <= 0")
	} else if p.Port > math.MaxUint16 {
		return fmt.Errorf("port is > %d", math.MaxUint16)
	}

	if len(p.Tables) == 0 {
		return fmt.Errorf("no tables passed in")
	}

	for _, table := range p.Tables {
		if table.Name == "" {
			return fmt.Errorf("table name must be passed in")
		}

		if table.Schema == "" {
			return fmt.Errorf("schema must be passed in")
		}
	}

	return nil
}
