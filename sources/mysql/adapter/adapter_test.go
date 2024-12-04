package adapter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/mysql"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

func TestMySQLAdapter_TableName(t *testing.T) {
	table := mysql.Table{
		Name: "table1",
	}
	adapter, err := newMySQLAdapter(nil, "foo", table, []schema.Column{}, scan.ScannerConfig{})
	assert.NoError(t, err)
	assert.Equal(t, "table1", adapter.TableName())
}

func TestMySQLAdapter_TopicSuffix(t *testing.T) {
	type _tc struct {
		table    mysql.Table
		expected string
	}

	tcs := []_tc{
		{
			table: mysql.Table{
				Name: "table1",
			},
			expected: "db.table1",
		},
		{
			table: mysql.Table{
				Name: "PublicStatus",
			},
			expected: "db.PublicStatus",
		},
	}

	for _, tc := range tcs {
		adapter, err := newMySQLAdapter(nil, "db", tc.table, []schema.Column{}, scan.ScannerConfig{})
		assert.NoError(t, err)
		assert.Equal(t, tc.expected, adapter.TopicSuffix())
	}
}
