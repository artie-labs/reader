package config

import (
	"fmt"
	"net/url"
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
