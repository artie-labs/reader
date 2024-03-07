package adapter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/postgres"
)

func TestPostgresAdapter_TableName(t *testing.T) {
	table := postgres.Table{
		Schema: "schema",
		Name:   "table1",
	}
	assert.Equal(t, "table1", postgresAdapter{table: table}.TableName())
}

func TestPostgresAdapter_TopicSuffix(t *testing.T) {
	type _tc struct {
		table             postgres.Table
		expectedTopicName string
	}

	tcs := []_tc{
		{
			table: postgres.Table{
				Name:   "table1",
				Schema: "schema1",
			},
			expectedTopicName: "schema1.table1",
		},
		{
			table: postgres.Table{
				Name:   `"PublicStatus"`,
				Schema: "schema2",
			},
			expectedTopicName: "schema2.PublicStatus",
		},
	}

	for _, tc := range tcs {
		adapter := postgresAdapter{table: tc.table}
		assert.Equal(t, tc.expectedTopicName, adapter.TopicSuffix())
	}
}

func TestPostgresAdapter_PartitionKey(t *testing.T) {
	type _tc struct {
		name     string
		keys     []string
		row      map[string]any
		expected map[string]any
	}

	tcs := []_tc{
		{
			name:     "no primary keys",
			row:      map[string]any{},
			expected: map[string]any{},
		},
		{
			name:     "primary keys - empty row",
			keys:     []string{"foo", "bar"},
			row:      map[string]any{},
			expected: map[string]any{"foo": nil, "bar": nil},
		},
		{
			name:     "primary keys - row has data",
			keys:     []string{"foo", "bar"},
			row:      map[string]any{"foo": "a", "bar": 2, "baz": 3},
			expected: map[string]any{"foo": "a", "bar": 2},
		},
	}

	for _, tc := range tcs {
		table := postgres.Table{
			Schema:      "schema",
			Name:        "tbl1",
			PrimaryKeys: tc.keys,
		}
		adapter := postgresAdapter{table: table}
		assert.Equal(t, tc.expected, adapter.PartitionKey(tc.row), tc.name)
	}
}
