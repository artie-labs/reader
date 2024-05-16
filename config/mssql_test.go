package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMSSQL_ToDSN(t *testing.T) {
	m := MSSQL{
		Host:     "localhost",
		Port:     1433,
		Username: "sa",
		Password: "ThisIsMyPassword!",
		Database: "master",
	}

	assert.Equal(t, "sqlserver://sa:ThisIsMyPassword%21@localhost:1433?database=master", m.ToDSN())
}

func TestMSSQL_Validate(t *testing.T) {
	{
		// Config is empty
		var m *MSSQL
		assert.ErrorContains(t, m.Validate(), "the MSSQL config is nil")
	}
	{
		// Host, username, password, database are empty
		p := &MSSQL{}
		assert.ErrorContains(t, p.Validate(), "one of the MSSQL settings is empty: host, username, password, database")
	}
	{
		// Port is -1
		m := &MSSQL{
			Host:     "host",
			Port:     -1,
			Username: "username",
			Password: "password",
			Database: "database",
			Tables: []*MSSQLTable{
				{
					Name:   "name",
					Schema: "schema",
				},
			},
		}

		assert.ErrorContains(t, m.Validate(), "port is not set or <= 0")
	}
	{
		// Port is 0
		m := &MSSQL{
			Host:     "host",
			Port:     -1,
			Username: "username",
			Password: "password",
			Database: "database",
			Tables: []*MSSQLTable{
				{
					Name:   "name",
					Schema: "schema",
				},
			},
		}

		assert.ErrorContains(t, m.Validate(), "port is not set or <= 0")
	}
	{
		// Port is too big
		p := &MSSQL{
			Host:     "host",
			Port:     1_000_000,
			Username: "username",
			Password: "password",
			Database: "database",
			Tables: []*MSSQLTable{
				{
					Name:   "name",
					Schema: "schema",
				},
			},
		}

		assert.ErrorContains(t, p.Validate(), "port is > 65535")
	}
	{
		// Tables are empty
		m := &MSSQL{
			Host:     "host",
			Port:     1,
			Username: "username",
			Password: "password",
			Database: "database",
		}

		assert.ErrorContains(t, m.Validate(), "no tables passed in")
	}
	{
		// No table name
		m := &MSSQL{
			Host:     "host",
			Port:     1,
			Username: "username",
			Password: "password",
			Database: "database",
			Tables: []*MSSQLTable{
				{
					Schema: "schema",
				},
			},
		}

		assert.ErrorContains(t, m.Validate(), "table name and schema must be passed in")
	}
	{
		// No schema name
		m := &MSSQL{
			Host:     "host",
			Port:     1,
			Username: "username",
			Password: "password",
			Database: "database",
			Tables: []*MSSQLTable{
				{
					Name: "name",
				},
			},
		}

		assert.ErrorContains(t, m.Validate(), "table name and schema must be passed in")
	}
	{
		// Valid
		m := &MSSQL{
			Host:     "host",
			Port:     1,
			Username: "username",
			Password: "password",
			Database: "database",
			Tables: []*MSSQLTable{
				{
					Name:   "name",
					Schema: "schema",
				},
			},
		}
		assert.NoError(t, m.Validate())
	}
}

func TestMSSQLTable_GetBatchSize(t *testing.T) {
	{
		// Batch size is not set
		m := &MSSQLTable{}
		assert.Equal(t, uint(5_000), m.GetBatchSize())
	}
	{
		// Batch size is set
		m := &MSSQLTable{
			BatchSize: 1,
		}
		assert.Equal(t, uint(1), m.GetBatchSize())
	}
}

func TestMSSQLTable_GetOptionalPrimaryKeyValStart(t *testing.T) {
	{
		// not set
		m := &MSSQLTable{}
		assert.Empty(t, m.GetOptionalPrimaryKeyValStart())
	}
	{
		// set
		m := &MSSQLTable{
			OptionalPrimaryKeyValStart: "a,b,c",
		}
		assert.Equal(t, []string{"a", "b", "c"}, m.GetOptionalPrimaryKeyValStart())
	}
}

func TestMSSQLTable_GetOptionalPrimaryKeyValEnd(t *testing.T) {
	{
		// not set
		m := &MSSQLTable{}
		assert.Empty(t, m.GetOptionalPrimaryKeyValEnd())
	}
	{
		// set
		m := &MSSQLTable{
			OptionalPrimaryKeyValEnd: "a,b,c",
		}
		assert.Equal(t, []string{"a", "b", "c"}, m.GetOptionalPrimaryKeyValEnd())
	}
}
