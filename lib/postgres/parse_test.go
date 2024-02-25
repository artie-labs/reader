package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/postgres/schema"
)

func TestParse(t *testing.T) {
	type _testCase struct {
		colName       string
		dataType      schema.DataType
		value         any
		expectErr     bool
		expectedValue any
	}

	tcs := []_testCase{
		{
			colName:       "bit_test (true)",
			dataType:      schema.Bit,
			value:         "1",
			expectedValue: true,
		},
		{
			colName:       "bit_test (false)",
			dataType:      schema.Bit,
			value:         "0",
			expectedValue: false,
		},
		{
			colName:       "foo",
			dataType:      schema.Array,
			value:         `["foo", "bar", "abc"]`,
			expectedValue: []any{"foo", "bar", "abc"},
		},
		{
			colName:       "group",
			dataType:      schema.Text,
			value:         "hello",
			expectedValue: "hello",
		},
		{
			colName:       "uuid",
			dataType:      schema.UUID,
			value:         "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
			expectedValue: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
		},
		{
			colName:       "json",
			dataType:      schema.JSON,
			value:         []byte(`{"foo":"bar"}`),
			expectedValue: `{"foo":"bar"}`,
		},
		{
			colName:  "geography",
			dataType: schema.Geography,
			value:    "0101000020E61000000000000000804B4000000000008040C0",
			expectedValue: map[string]any{
				"srid": nil,
				"wkb":  "AQEAACDmEAAAAAAAAACAS0AAAAAAAIBAwA==",
			},
		},
	}

	for _, tc := range tcs {
		value, err := ParseValue(tc.dataType, tc.value)

		if tc.expectErr {
			assert.Error(t, err, tc.colName)
		} else {
			assert.NoError(t, err, tc.colName)
			assert.Equal(t, tc.expectedValue, value, tc.colName)
		}
	}
}
