package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/mysql/schema"
)

func TestGetColumnByName(t *testing.T) {
	type _tc struct {
		table          *Table
		columnName     string
		expectedResult *schema.Column
		expectedErr    string
	}

	tcs := []_tc{
		{
			table: &Table{
				Columns: []schema.Column{},
			},
			columnName:  "col2",
			expectedErr: "failed to find column with name col2",
		},
		{
			table: &Table{
				Columns: []schema.Column{
					{
						Name: "col1",
						Type: schema.Text,
					},
					{
						Name: "col2",
						Type: schema.BigInt,
					},
					{
						Name: "col3",
						Type: schema.Binary,
					},
				},
			},
			columnName: "col2",
			expectedResult: &schema.Column{
				Name: "col2",
				Type: schema.BigInt,
			},
		},
		{
			table: &Table{
				Columns: []schema.Column{
					{
						Name: "col1",
						Type: schema.Text,
					},
				},
			},
			columnName:  "col2",
			expectedErr: "failed to find column with name col2",
		},
	}

	for _, tc := range tcs {
		result, err := tc.table.GetColumnByName(tc.columnName)
		if tc.expectedErr != "" {
			assert.ErrorContains(t, err, tc.expectedErr)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, tc.expectedResult, result)
		}
	}
}

func TestGetColumnsByName(t *testing.T) {
	type _tc struct {
		table          *Table
		columnNames    []string
		expectedResult []schema.Column
		expectedErr    string
	}

	tcs := []_tc{
		{
			table:          &Table{},
			columnNames:    []string{},
			expectedResult: []schema.Column(nil),
		},
		{
			table: &Table{
				Columns: []schema.Column{
					{
						Name: "col1",
						Type: schema.Text,
					},
					{
						Name: "col2",
						Type: schema.Blob,
					},
					{
						Name: "col3",
						Type: schema.Date,
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
					Type: schema.Date,
				},
			},
		},
		{
			table: &Table{
				Columns: []schema.Column{
					{
						Name: "col1",
						Type: schema.Text,
					},
				},
			},
			columnNames: []string{"col1", "col2"},
			expectedErr: "failed to find column with name col2",
		},
	}

	for _, tc := range tcs {
		result, err := tc.table.GetColumnsByName(tc.columnNames)
		if tc.expectedErr != "" {
			assert.ErrorContains(t, err, tc.expectedErr)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, tc.expectedResult, result)
		}
	}
}
