package config

import (
	"fmt"
	"github.com/artie-labs/reader/constants"
	"github.com/artie-labs/reader/lib/rdbms/scan"
	"github.com/artie-labs/transfer/lib/stringutil"
	"math"
	"net/url"
	"strings"
)

type MSSQL struct {
	Host     string        `yaml:"host"`
	Port     int           `yaml:"port"`
	Username string        `yaml:"username"`
	Password string        `yaml:"password"`
	Database string        `yaml:"database"`
	Tables   []*MSSQLTable `yaml:"tables"`
}

type MSSQLTable struct {
	Name                       string   `yaml:"name"`
	Schema                     string   `yaml:"schema"`
	BatchSize                  uint     `yaml:"batchSize"`
	OptionalPrimaryKeyValStart string   `yaml:"optionalPrimaryKeyValStart"`
	OptionalPrimaryKeyValEnd   string   `yaml:"optionalPrimaryKeyValEnd"`
	ExcludeColumns             []string `yaml:"excludeColumns"`
}

func (m *MSSQL) ToDSN() string {
	query := url.Values{}
	query.Add("database", m.Database)

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(m.Username, m.Password),
		Host:     fmt.Sprintf("%s:%d", m.Host, m.Port),
		RawQuery: query.Encode(),
	}

	return u.String()
}

func (m *MSSQLTable) GetBatchSize() uint {
	if m.BatchSize > 0 {
		return m.BatchSize
	} else {
		return constants.DefaultBatchSize
	}
}

func (m *MSSQLTable) GetOptionalPrimaryKeyValStart() []string {
	if m.OptionalPrimaryKeyValStart == "" {
		return []string{}
	}
	return strings.Split(m.OptionalPrimaryKeyValStart, ",")
}

func (m *MSSQLTable) GetOptionalPrimaryKeyValEnd() []string {
	if m.OptionalPrimaryKeyValEnd == "" {
		return []string{}
	}
	return strings.Split(m.OptionalPrimaryKeyValEnd, ",")
}

func (m *MSSQLTable) ToScannerConfig(errorRetries int) scan.ScannerConfig {
	return scan.ScannerConfig{
		BatchSize:              m.GetBatchSize(),
		OptionalStartingValues: m.GetOptionalPrimaryKeyValStart(),
		OptionalEndingValues:   m.GetOptionalPrimaryKeyValEnd(),
		ErrorRetries:           errorRetries,
	}
}

func (m *MSSQL) Validate() error {
	if m == nil {
		return fmt.Errorf("the PostgreSQL config is nil")
	}

	if stringutil.Empty(m.Host, m.Username, m.Password, m.Database) {
		return fmt.Errorf("one of the MSSQL settings is empty: host, username, password, database")
	}

	if m.Port <= 0 {
		return fmt.Errorf("port is not set or <= 0")
	} else if m.Port > math.MaxUint16 {
		return fmt.Errorf("port is > %d", math.MaxUint16)
	}

	if len(m.Tables) == 0 {
		return fmt.Errorf("no tables passed in")
	}

	for _, table := range m.Tables {
		if stringutil.Empty(table.Name, table.Schema) {
			return fmt.Errorf("table name and schema must be passed in")
		}
	}

	return nil
}
