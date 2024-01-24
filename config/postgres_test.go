package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresValidate(t *testing.T) {
	{
		// Config is empty
		var p *PostgreSQL
		assert.ErrorContains(t, p.Validate(), "postgres config is nil")
	}
	{
		// Host, port, username, password, database are empty
		p := &PostgreSQL{}
		assert.ErrorContains(t, p.Validate(), "one of the postgresql settings is empty: host, port, username, password, database")
	}
	{
		// Tables are empty
		p := &PostgreSQL{
			Host:     "host",
			Port:     "port",
			Username: "username",
			Password: "password",
			Database: "database",
		}

		assert.ErrorContains(t, p.Validate(), "no tables passed in")
	}
	{
		// No table name
		p := &PostgreSQL{
			Host:     "host",
			Port:     "port",
			Username: "username",
			Password: "password",
			Database: "database",
			Tables: []*PostgreSQLTable{
				{
					Schema: "schema",
				},
			},
		}

		assert.ErrorContains(t, p.Validate(), "table name must be passed in")
	}
	{
		// No schema name
		p := &PostgreSQL{
			Host:     "host",
			Port:     "port",
			Username: "username",
			Password: "password",
			Database: "database",
			Tables: []*PostgreSQLTable{
				{
					Name: "name",
				},
			},
		}

		assert.ErrorContains(t, p.Validate(), "schema must be passed in")
	}
	{
		// Valid
		p := &PostgreSQL{
			Host:     "host",
			Port:     "port",
			Username: "username",
			Password: "password",
			Database: "database",
			Tables: []*PostgreSQLTable{
				{
					Name:   "name",
					Schema: "schema",
				},
			},
		}
		assert.NoError(t, p.Validate())
	}
}
