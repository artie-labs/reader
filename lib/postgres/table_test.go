package postgres

import (
	"testing"

	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/stretchr/testify/assert"
)

func TestTopicSuffix(t *testing.T) {
	type _tc struct {
		table             *Table
		expectedTopicName string
	}

	tcs := []_tc{
		{
			table: &Table{
				Name:   "table1",
				Schema: "schema1",
			},
			expectedTopicName: "schema1.table1",
		},
		{
			table: &Table{
				Name:   `"PublicStatus"`,
				Schema: "schema2",
			},
			expectedTopicName: "schema2.PublicStatus",
		},
	}

	for _, tc := range tcs {
		assert.Equal(t, tc.expectedTopicName, tc.table.TopicSuffix())
	}
}

func TestGetColumnsByName(t *testing.T) {
	type _tc struct {
		table          *Table
		columnNames    []string
		expectedResult []schema.Column
	}

	tcs := []_tc{
		{
			table: &Table{
				Columns: []schema.Column{
					{
						Name: "col1",
						Type: schema.Text,
					},
					{
						Name: "col2",
						Type: schema.VariableNumeric,
					},
					{
						Name: "col3",
						Type: schema.Array,
					},
				},
			},
			columnNames: []string{"col1", "col3"},
			expectedResult: []schema.Column{
				{
					Name: "col1",
					Type: schema.Text,
				},
				{
					Name: "col3",
					Type: schema.Array,
				},
			},
		},
	}

	for _, tc := range tcs {
		result, err := tc.table.GetColumnsByName(tc.columnNames)
		assert.Nil(t, err)
		assert.Equal(t, tc.expectedResult, result)
	}
}
