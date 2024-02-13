package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostgreSQL_Validate(t *testing.T) {
	{
		// Config is empty
		var p *PostgreSQL
		assert.ErrorContains(t, p.Validate(), "the PostgreSQL config is nil")
	}
	{
		// Host, username, password, database are empty
		p := &PostgreSQL{}
		assert.ErrorContains(t, p.Validate(), "one of the PostgreSQL settings is empty: host, username, password, database")
	}
	{
		// Port is -1
		p := &PostgreSQL{
			Host:     "host",
			Port:     -1,
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

		assert.ErrorContains(t, p.Validate(), "port is not set or <= 0")
	}
	{
		// Port is 0
		p := &PostgreSQL{
			Host:     "host",
			Port:     -1,
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

		assert.ErrorContains(t, p.Validate(), "port is not set or <= 0")
	}
	{
		// Port is too big
		p := &PostgreSQL{
			Host:     "host",
			Port:     1_000_000,
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

		assert.ErrorContains(t, p.Validate(), "port is > than 65535")
	}
	{
		// Tables are empty
		p := &PostgreSQL{
			Host:     "host",
			Port:     1,
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
			Port:     1,
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
			Port:     1,
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
			Port:     1,
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

func TestPostgreSQL_GetUsername(t *testing.T) {
	{
		// Username is set, legacy username is not set
		p := &PostgreSQL{
			Username: "username",
		}
		assert.Equal(t, "username", p.GetUsername())
	}
	{
		// Legacy username is set, username is not set
		p := &PostgreSQL{
			LegacyUsername: "legacyUsername",
		}
		assert.Equal(t, "legacyUsername", p.GetUsername())
	}
	{
		// Legacy username is set and username is set
		p := &PostgreSQL{
			Username:       "username",
			LegacyUsername: "legacyUsername",
		}
		assert.Equal(t, "username", p.GetUsername())
	}
}

func TestPostgreSQLTable_GetBatchSize(t *testing.T) {
	{
		// Batch size is not set
		p := &PostgreSQLTable{}
		assert.Equal(t, uint(5_000), p.GetBatchSize())
	}
	{
		// Batch size is set
		p := &PostgreSQLTable{
			BatchSize: 1,
		}
		assert.Equal(t, uint(1), p.GetBatchSize())
	}
}
