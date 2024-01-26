package debezium

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestParseValue(t *testing.T) {
	type _tc struct {
		name          string
		key           string
		value         interface{}
		config        *Fields
		numericValue  bool
		expectedValue interface{}
		expectErr     bool
	}

	dateFields := NewFields()
	dateFields.AddField("date_col", Date, nil)

	numericFields := NewFields()
	numericFields.AddField("numeric_col", Numeric, &Opts{
		Scale:     ptr.ToString("2"),
		Precision: ptr.ToString("5"),
	})

	moneyFields := NewFields()
	moneyFields.AddField("money_col", Money, &Opts{
		Scale: ptr.ToString("2"),
	})

	varNumericFields := NewFields()
	varNumericFields.AddField("variable_numeric_col", VariableNumeric, nil)

	tcs := []_tc{
		{
			name:   "nil value",
			key:    "user_id",
			value:  nil,
			config: NewFields(),
		},
		{
			name:          "date (postgres.Date)",
			key:           "date_col",
			value:         time.Date(2023, 5, 3, 0, 0, 0, 0, time.UTC),
			config:        dateFields,
			expectedValue: 19480,
		},
		{
			name:          "numeric (postgres.Numeric)",
			key:           "numeric_col",
			value:         578.01,
			numericValue:  true,
			config:        numericFields,
			expectedValue: "578.01",
		},
		{
			name:          "numeric (postgres.Numeric) - money",
			key:           "money_col",
			config:        moneyFields,
			numericValue:  true,
			value:         123.99,
			expectedValue: "123.99",
		},
		{
			name:          "numeric (postgres.Numeric) - variable numeric",
			key:           "variable_numeric_col",
			config:        varNumericFields,
			value:         123.98,
			expectedValue: map[string]string{"scale": "2", "value": "MG4="},
		},
		{
			name:          "string",
			key:           "name",
			value:         "name",
			config:        NewFields(),
			expectedValue: "name",
		},
		{
			name:          "boolean",
			key:           "bool",
			value:         true,
			config:        NewFields(),
			expectedValue: true,
		},
	}

	for _, tc := range tcs {
		actualValue, actualErr := ParseValue(tc.key, tc.value, tc.config)
		if tc.expectErr {
			assert.Error(t, actualErr, tc.name)
		} else {
			assert.NoError(t, actualErr, tc.name)
			if tc.numericValue {
				field, isOk := tc.config.GetField(tc.key)
				assert.True(t, isOk, tc.name)

				val, err := field.DecodeDecimal(fmt.Sprint(actualValue))
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedValue, val.String(), tc.name)
			} else {
				assert.Equal(t, tc.expectedValue, actualValue, tc.name)
			}
		}
	}
}
