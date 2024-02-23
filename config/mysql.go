package config

import (
	"fmt"
	"math"
	"strings"

	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/go-sql-driver/mysql"

	"github.com/artie-labs/reader/constants"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

type MySQL struct {
	Host     string        `yaml:"host"`
	Port     int           `yaml:"port"`
	Username string        `yaml:"username"`
	Password string        `yaml:"password"`
	Database string        `yaml:"database"`
	Tables   []*MySQLTable `yaml:"tables"`
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
	Name                       string `yaml:"name"`
	BatchSize                  uint   `yaml:"batchSize"`
	OptionalPrimaryKeyValStart string `yaml:"optionalPrimaryKeyValStart"`
	OptionalPrimaryKeyValEnd   string `yaml:"optionalPrimaryKeyValEnd"`
}

func (m *MySQLTable) GetBatchSize() uint {
	if m.BatchSize > 0 {
		return m.BatchSize
	} else {
		return constants.DefaultBatchSize
	}
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
	}

	return nil
}
