package postgres

import (
	"testing"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/postgres/schema"
)

func TestParse(t *testing.T) {
	type _testCase struct {
		colName       string
		colKind       string
		udtName       *string
		value         any
		expectErr     bool
		expectedValue any
	}

	tcs := []_testCase{
		{
			colName:       "bit_test (true)",
			colKind:       "bit",
			value:         "1",
			expectedValue: true,
		},
		{
			colName:       "bit_test (false)",
			colKind:       "bit",
			value:         "0",
			expectedValue: false,
		},
		{
			colName:       "foo",
			colKind:       "ARRAY",
			value:         `["foo", "bar", "abc"]`,
			expectedValue: []any{"foo", "bar", "abc"},
		},
		{
			colName:       "group",
			colKind:       "character varying",
			value:         "hello",
			expectedValue: "hello",
		},
		{
			colName:       "json",
			colKind:       "json",
			value:         []byte(`{"foo":"bar"}`),
			expectedValue: `{"foo":"bar"}`,
		},
		{
			colName: "geography",
			colKind: "user-defined",
			udtName: ptr.ToString("geography"),
			value:   "0101000020E61000000000000000804B4000000000008040C0",
			expectedValue: map[string]any{
				"srid": nil,
				"wkb":  "AQEAACDmEAAAAAAAAACAS0AAAAAAAIBAwA==",
			},
		},
	}

	for _, tc := range tcs {
		dataType, _ := schema.ParseColumnDataType(tc.colKind, nil, nil, tc.udtName)

		value, err := ParseValue(dataType, tc.value)

		if tc.expectErr {
			assert.Error(t, err, tc.colName)
		} else {
			assert.NoError(t, err, tc.colName)
			assert.Equal(t, tc.expectedValue, value, tc.colName)
		}
	}
}
