package config

import (
	"cmp"
	"fmt"
	"math"
	"strings"

	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/go-sql-driver/mysql"

	"github.com/artie-labs/reader/constants"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

type MySQLStreamingSettings struct {
	Enabled           bool   `yaml:"enabled,omitempty"`
	OffsetFile        string `yaml:"offsetFile,omitempty"`
	SchemaHistoryFile string `yaml:"schemaHistoryFile,omitempty"`
	// ServerID - Unique ID in the cluster.
	ServerID uint32 `yaml:"serverID,omitempty"`
}

type MySQL struct {
	Host              string                 `yaml:"host"`
	Port              int                    `yaml:"port"`
	Username          string                 `yaml:"username"`
	Password          string                 `yaml:"password"`
	Database          string                 `yaml:"database"`
	Tables            []*MySQLTable          `yaml:"tables"`
	StreamingSettings MySQLStreamingSettings `yaml:"streamingSettings,omitempty"`
}

func (m *MySQL) ToDSN() string {
	config := mysql.NewConfig()
	config.User = m.Username
	config.Passwd = m.Password
	config.Net = "tcp"
	config.Addr = fmt.Sprintf("%s:%d", m.Host, m.Port)
	config.DBName = m.Database
	return config.FormatDSN()
}

type MySQLTable struct {
	Name string `yaml:"name"`
	// Optional settings
	BatchSize                  uint     `yaml:"batchSize,omitempty"`
	OptionalPrimaryKeyValStart string   `yaml:"optionalPrimaryKeyValStart,omitempty"`
	OptionalPrimaryKeyValEnd   string   `yaml:"optionalPrimaryKeyValEnd,omitempty"`
	ExcludeColumns             []string `yaml:"excludeColumns,omitempty"`
	// IncludeColumns - List of columns that should be included in the change event record.
	IncludeColumns []string `yaml:"includeColumns,omitempty"`
}

func (m *MySQLTable) GetBatchSize() uint {
	return cmp.Or(m.BatchSize, constants.DefaultBatchSize)
}

func (m *MySQLTable) GetOptionalPrimaryKeyValStart() []string {
	if m.OptionalPrimaryKeyValStart == "" {
		return []string{}
	}
	return strings.Split(m.OptionalPrimaryKeyValStart, ",")
}

func (m *MySQLTable) GetOptionalPrimaryKeyValEnd() []string {
	if m.OptionalPrimaryKeyValEnd == "" {
		return []string{}
	}
	return strings.Split(m.OptionalPrimaryKeyValEnd, ",")
}

func (m *MySQLTable) ToScannerConfig(errorRetries int) scan.ScannerConfig {
	return scan.ScannerConfig{
		BatchSize:              m.GetBatchSize(),
		OptionalStartingValues: m.GetOptionalPrimaryKeyValStart(),
		OptionalEndingValues:   m.GetOptionalPrimaryKeyValEnd(),
		ErrorRetries:           errorRetries,
	}
}

func (m *MySQL) Validate() error {
	if m == nil {
		return fmt.Errorf("MySQL config is nil")
	}

	if stringutil.Empty(m.Host, m.Username, m.Password, m.Database) {
		return fmt.Errorf("one of the MySQL settings is empty: host, username, password, database")
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
		if table.Name == "" {
			return fmt.Errorf("table name must be passed in")
		}

		// You should not be able to filter and exclude columns at the same time
		if len(table.ExcludeColumns) > 0 && len(table.IncludeColumns) > 0 {
			return fmt.Errorf("cannot exclude and include columns at the same time")
		}
	}

	return nil
}
