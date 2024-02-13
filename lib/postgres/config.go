package postgres

import (
	"github.com/artie-labs/reader/lib/postgres/debezium"
)

type Config struct {
	Fields *debezium.Fields
}

func NewPostgresConfig() *Config {
	return &Config{Fields: debezium.NewFields()}
}
