package config

type Postgres struct {
	Host     string           `yaml:"host"`
	Port     string           `yaml:"port"`
	Username string           `yaml:"userName"`
	Password string           `yaml:"password"`
	Database string           `yaml:"database"`
	Tables   []*PostgresTable `yaml:"tables"`
}

type PostgresTable struct {
	Name                       string `yaml:"name"`
	Schema                     string `yaml:"schema"`
	Limit                      uint   `yaml:"limit"`
	OptionalPrimaryKeyValStart string `yaml:"optionalPrimaryKeyValStart"`
	OptionalPrimaryKeyValEnd   string `yaml:"optionalPrimaryKeyValEnd"`
}
