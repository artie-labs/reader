package adapter

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/postgres/schema"
)

func TestConvertValueToDebezium(t *testing.T) {
	type _tc struct {
		name          string
		col           schema.Column
		value         any
		numericValue  bool
		expectedValue any
		expectErr     bool
	}

	tcs := []_tc{
		{
			name:  "nil value",
			value: nil,
		},
		{
			name:          "date (postgres.Date)",
			col:           schema.Column{Name: "date_col", Type: schema.Date},
			value:         time.Date(2023, 5, 3, 0, 0, 0, 0, time.UTC),
			expectedValue: 19480,
		},
		{
			name: "numeric (postgres.Numeric)",
			col: schema.Column{Name: "numeric_col", Type: schema.Numeric, Opts: &schema.Opts{
				Scale:     2,
				Precision: 5,
			}},
			value:         "578.01",
			numericValue:  true,
			expectedValue: "578.01",
		},
		{
			name:          "numeric (postgres.Numeric) - money",
			col:           schema.Column{Name: "money_col", Type: schema.Money},
			numericValue:  true,
			value:         123.99,
			expectedValue: "123.99",
		},
		{
			name:          "numeric (postgres.Numeric) - variable numeric",
			col:           schema.Column{Name: "variable_numeric_col", Type: schema.VariableNumeric},
			value:         "123.98",
			expectedValue: map[string]string{"scale": "2", "value": "MG4="},
		},
		{
			name:          "string",
			col:           schema.Column{Name: "name", Type: schema.Text},
			value:         "name",
			expectedValue: "name",
		},
		{
			name:          "boolean",
			col:           schema.Column{Name: "bool", Type: schema.Boolean},
			value:         true,
			expectedValue: true,
		},
	}

	for _, tc := range tcs {
		actualValue, actualErr := ConvertValueToDebezium(tc.col, tc.value)
		if tc.expectErr {
			assert.Error(t, actualErr, tc.name)
		} else {
			assert.NoError(t, actualErr, tc.name)
			if tc.numericValue {
				field, err := ColumnToField(tc.col)
				assert.NoError(t, err, tc.name)
				val, err := field.DecodeDecimal(fmt.Sprint(actualValue))
				assert.NoError(t, err, tc.name)
				assert.Equal(t, tc.expectedValue, val.String(), tc.name)
			} else {
				assert.Equal(t, tc.expectedValue, actualValue, tc.name)
			}
		}
	}
}
