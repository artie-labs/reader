package adapter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/mysql"
)

func TestMySQLAdapter_TableName(t *testing.T) {
	table := mysql.Table{
		Name: "table1",
	}
	assert.Equal(t, "table1", NewMySQLAdapter(table).TableName())
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
			expected: "table1",
		},
		{
			table: mysql.Table{
				Name: `"PublicStatus"`,
			},
			expected: "PublicStatus",
		},
	}

	for _, tc := range tcs {
		adapter := NewMySQLAdapter(tc.table)
		assert.Equal(t, tc.expected, adapter.TopicSuffix())
	}
}

func TestMySQLAdapter_PartitionKey(t *testing.T) {
	type _tc struct {
		name     string
		keys     []string
		row      map[string]interface{}
		expected map[string]interface{}
	}

	tcs := []_tc{
		{
			name:     "no primary keys",
			keys:     []string{},
			row:      map[string]interface{}{},
			expected: map[string]interface{}{},
		},
		{
			name:     "primary keys - empty row",
			keys:     []string{"foo", "bar"},
			row:      map[string]interface{}{},
			expected: map[string]interface{}{"foo": nil, "bar": nil},
		},
		{
			name:     "primary keys - row has data",
			keys:     []string{"foo", "bar"},
			row:      map[string]interface{}{"foo": "a", "bar": 2, "baz": 3},
			expected: map[string]interface{}{"foo": "a", "bar": 2},
		},
	}

	for _, tc := range tcs {
		table := mysql.NewTable("tbl1")
		for _, key := range tc.keys {
			table.PrimaryKeys.Upsert(key, nil, nil)
		}
		adapter := NewMySQLAdapter(*table)
		assert.Equal(t, tc.expected, adapter.PartitionKey(tc.row), tc.name)
	}
}
