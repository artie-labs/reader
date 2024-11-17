package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func createValidConfig() *MySQL {
	return &MySQL{
		Host:     "example.com",
		Port:     3306,
		Username: "username",
		Password: "password",
		Database: "database",
		Tables: []MySQLTable{
			{
				Name:                       "table1",
				BatchSize:                  100,
				OptionalPrimaryKeyValStart: "start",
				OptionalPrimaryKeyValEnd:   "end",
			},
			{
				Name:                       "table2",
				BatchSize:                  200,
				OptionalPrimaryKeyValStart: "start",
				OptionalPrimaryKeyValEnd:   "end",
			},
		},
	}
}

func TestMySQL_Validate(t *testing.T) {
	{
		// config is empty
		var c *MySQL
		assert.ErrorContains(t, c.Validate(), "MySQL config is nil")
	}
	{
		// happy path
		assert.NoError(t, createValidConfig().Validate())
	}
	{
		// empty host
		c := createValidConfig()
		c.Host = ""
		assert.ErrorContains(t, c.Validate(), "one of the MySQL settings is empty: host, username, password, database")
	}
	{
		// empty user
		c := createValidConfig()
		c.Username = ""
		assert.ErrorContains(t, c.Validate(), "one of the MySQL settings is empty: host, username, password, database")
	}
	{
		// empty password
		c := createValidConfig()
		c.Password = ""
		assert.ErrorContains(t, c.Validate(), "one of the MySQL settings is empty: host, username, password, database")
	}
	{
		// empty database
		c := createValidConfig()
		c.Database = ""
		assert.ErrorContains(t, c.Validate(), "one of the MySQL settings is empty: host, username, password, database")
	}
	{
		// bad port - negative
		c := createValidConfig()
		c.Port = -2
		assert.ErrorContains(t, c.Validate(), "port is not set or <= 0")
	}
	{
		// bad port 0 9
		c := createValidConfig()
		c.Port = 0
		assert.ErrorContains(t, c.Validate(), "port is not set or <= 0")
	}
	{
		// bad port - too large
		c := createValidConfig()
		c.Port = 1_000_000
		assert.ErrorContains(t, c.Validate(), "port is > 65535")
	}
	{
		// no tables
		c := createValidConfig()
		c.Tables = nil
		assert.ErrorContains(t, c.Validate(), "no tables passed in")
		c.Tables = []MySQLTable{}
		assert.ErrorContains(t, c.Validate(), "no tables passed in")
	}
	{
		// missing table name
		c := createValidConfig()
		c.Tables = append(c.Tables, MySQLTable{})
		assert.ErrorContains(t, c.Validate(), "table name must be passed in")
	}
	{
		// exclude and include at the same time
		c := createValidConfig()
		c.Tables = append(c.Tables, MySQLTable{
			Name:           "foo",
			IncludeColumns: []string{"foo"},
			ExcludeColumns: []string{"bar"},
		})

		assert.ErrorContains(t, c.Validate(), "cannot exclude and include columns at the same time")
	}
}

func TestMySQL_ToDSN(t *testing.T) {
	c := createValidConfig()
	assert.Equal(t, "username:password@tcp(example.com:3306)/database", c.ToDSN())
}

func TestMySQLTable_GetBatchSize(t *testing.T) {
	{
		// Batch size is not set
		assert.Equal(t, uint(5_000), MySQLTable{}.GetBatchSize())
	}
	{
		// Batch size is set
		assert.Equal(t, uint(1), MySQLTable{BatchSize: 1}.GetBatchSize())
	}
}

func TestMySQLTable_GetOptionalPrimaryKeyValStart(t *testing.T) {
	{
		// not set
		assert.Len(t, MySQLTable{}.GetOptionalPrimaryKeyValStart(), 0)
	}
	{
		// set
		assert.Equal(t, []string{"a", "b", "c"}, MySQLTable{OptionalPrimaryKeyValStart: "a,b,c"}.GetOptionalPrimaryKeyValStart())
	}
}

func TestMySQLTable_GetOptionalPrimaryKeyValEnd(t *testing.T) {
	{
		// not set
		assert.Len(t, MySQLTable{}.GetOptionalPrimaryKeyValEnd(), 0)
	}
	{
		// set
		assert.Equal(t, []string{"a", "b", "c"}, MySQLTable{OptionalPrimaryKeyValEnd: "a,b,c"}.GetOptionalPrimaryKeyValEnd())
	}
}
