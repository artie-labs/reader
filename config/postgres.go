package config

import (
	"cmp"
	"fmt"
	"math"
	"strings"

	"github.com/artie-labs/transfer/lib/stringutil"

	"github.com/artie-labs/reader/constants"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

type PostgreSQL struct {
	Host       string             `yaml:"host"`
	Port       int                `yaml:"port"`
	Username   string             `yaml:"username"`
	Password   string             `yaml:"password"`
	Database   string             `yaml:"database"`
	Tables     []*PostgreSQLTable `yaml:"tables"`
	DisableSSL bool               `yaml:"disableSSL"`
}

func (p *PostgreSQL) ToDSN() string {
	connString := fmt.Sprintf("user=%s dbname=%s password=%s port=%d host=%s",
		p.Username, p.Database, p.Password, p.Port, p.Host)

	if p.DisableSSL {
		connString = fmt.Sprintf("%s sslmode=disable", connString)
	}

	return connString
}

type PostgreSQLTable struct {
	Name   string `yaml:"name"`
	Schema string `yaml:"schema"`

	// Optional settings
	BatchSize                  uint     `yaml:"batchSize,omitempty"`
	PrimaryKeysOverride        []string `yaml:"primaryKeysOverride,omitempty"`
	OptionalPrimaryKeyValStart string   `yaml:"optionalPrimaryKeyValStart,omitempty"`
	OptionalPrimaryKeyValEnd   string   `yaml:"optionalPrimaryKeyValEnd,omitempty"`
	ExcludeColumns             []string `yaml:"excludeColumns,omitempty"`
}

func (p *PostgreSQLTable) GetBatchSize() uint {
	return cmp.Or(p.BatchSize, constants.DefaultBatchSize)
}

func (p *PostgreSQLTable) GetOptionalPrimaryKeyValStart() []string {
	if p.OptionalPrimaryKeyValStart == "" {
		return []string{}
	}
	return strings.Split(p.OptionalPrimaryKeyValStart, ",")
}

func (p *PostgreSQLTable) GetOptionalPrimaryKeyValEnd() []string {
	if p.OptionalPrimaryKeyValEnd == "" {
		return []string{}
	}
	return strings.Split(p.OptionalPrimaryKeyValEnd, ",")
}

func (p *PostgreSQLTable) ToScannerConfig(errorRetries int) scan.ScannerConfig {
	return scan.ScannerConfig{
		BatchSize:              p.GetBatchSize(),
		OptionalStartingValues: p.GetOptionalPrimaryKeyValStart(),
		OptionalEndingValues:   p.GetOptionalPrimaryKeyValEnd(),
		ErrorRetries:           errorRetries,
	}
}

func (p *PostgreSQL) Validate() error {
	if p == nil {
		return fmt.Errorf("the PostgreSQL config is nil")
	}

	if stringutil.Empty(p.Host, p.Username, p.Password, p.Database) {
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
