package config

import (
	"fmt"
	"github.com/artie-labs/reader/constants"
	"github.com/artie-labs/reader/lib/rdbms/scan"
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

func (m MSSQL) ToDSN() string {
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
