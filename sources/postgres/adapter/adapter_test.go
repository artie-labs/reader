package adapter

import (
	"testing"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/postgres"
	"github.com/artie-labs/reader/lib/postgres/schema"
)

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
		adapter := NewPostgresAdapter(tc.table)
		assert.Equal(t, tc.expectedTopicName, adapter.TopicSuffix())
	}
}

func TestPostgresAdapter_Fields(t *testing.T) {
	table := postgres.Table{
		Name:   "table1",
		Schema: "schema1",
		Columns: []schema.Column{
			{Name: "col1", Type: schema.Text},
			{Name: "col2", Type: schema.Boolean},
			{Name: "col3", Type: schema.Array},
		},
	}
	adapter := NewPostgresAdapter(table)

	expected := []debezium.Field{
		{Type: "string", FieldName: "col1"},
		{Type: "boolean", FieldName: "col2"},
		{Type: "array", FieldName: "col3"},
	}
	assert.Equal(t, expected, adapter.Fields())
}
