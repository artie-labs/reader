package config

import (
	"fmt"
	"github.com/artie-labs/reader/constants"
	"github.com/artie-labs/transfer/lib/stringutil"
)

type PostgreSQL struct {
	Host       string             `yaml:"host"`
	Port       string             `yaml:"port"`
	Username   string             `yaml:"userName"`
	Password   string             `yaml:"password"`
	Database   string             `yaml:"database"`
	Tables     []*PostgreSQLTable `yaml:"tables"`
	DisableSSL bool               `yaml:"disableSSL"`
}

type PostgreSQLTable struct {
	Name                       string `yaml:"name"`
	Schema                     string `yaml:"schema"`
	Limit                      uint   `yaml:"limit"`
	OptionalPrimaryKeyValStart string `yaml:"optionalPrimaryKeyValStart"`
	OptionalPrimaryKeyValEnd   string `yaml:"optionalPrimaryKeyValEnd"`
}

func (p *PostgreSQLTable) GetLimit() uint {
	if p.Limit == 0 {
		return constants.DefaultLimit
	}

	return p.Limit
}

func (p *PostgreSQL) Validate() error {
	if p == nil {
		return fmt.Errorf("postgres config is nil")
	}

	if stringutil.Empty(p.Host, p.Port, p.Username, p.Password, p.Database) {
		return fmt.Errorf("one of the postgresql settings is empty: host, port, username, password, database")
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
