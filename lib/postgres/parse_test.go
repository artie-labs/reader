package postgres

import (
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/postgres/schema"
)

func TestParse(t *testing.T) {
	type _testCase struct {
		colName       string
		colKind       string
		udtName       *string
		parseTime     bool
		value         ValueWrapper
		expectErr     bool
		expectedValue interface{}
	}

	tcs := []_testCase{
		{
			colName: "bit_test (true)",
			colKind: "bit",
			value: ValueWrapper{
				Value: "1",
			},
			expectedValue: true,
		},
		{
			colName: "bit_test (false)",
			colKind: "bit",
			value: ValueWrapper{
				Value: "0",
			},
			expectedValue: false,
		},
		{
			colName: "foo",
			colKind: "ARRAY",
			value: ValueWrapper{
				Value: `["foo", "bar", "abc"]`,
			},
			expectedValue: []interface{}{"foo", "bar", "abc"},
		},
		{
			colName: "group",
			colKind: "character varying",
			value: ValueWrapper{
				Value: "hello",
			},
			expectedValue: "hello",
		},
		{
			colName: "uuid (already parsed, so skip parsing)",
			colKind: "uuid",
			value: ValueWrapper{
				Value:  "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
				parsed: true,
			},
			expectedValue: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
		},
		{
			colName: "parse time",
			colKind: "timestamp without time zone",
			value: ValueWrapper{
				Value: time.Date(1993, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			parseTime:     true,
			expectedValue: "1993-01-01T00:00:00Z",
		},
		{
			colName: "json",
			colKind: "json",
			value: ValueWrapper{
				Value: []byte(`{"foo":"bar"}`),
			},
			expectedValue: `{"foo":"bar"}`,
		},
		{
			colName: "geography",
			colKind: "user-defined",
			udtName: ptr.ToString("geography"),
			value: ValueWrapper{
				Value: "0101000020E61000000000000000804B4000000000008040C0",
			},
			expectedValue: map[string]interface{}{
				"srid": nil,
				"wkb":  "AQEAACDmEAAAAAAAAACAS0AAAAAAAIBAwA==",
			},
		},
	}

	for _, tc := range tcs {
		dataType, _ := schema.ParseColumnDataType(tc.colKind, nil, nil, tc.udtName)

		value, err := ParseValue(dataType, ParseValueArgs{
			ValueWrapper: tc.value,
			ParseTime:    tc.parseTime,
		})

		if tc.expectErr {
			assert.Error(t, err, tc.colName)
		} else {
			assert.NoError(t, err, tc.colName)
			assert.Equal(t, tc.expectedValue, value.Value, tc.colName)

			// if there are no errors, let's iterate over this a few times to make sure it's deterministic.
			for i := 0; i < 5; i++ {
				value, err = ParseValue(dataType, ParseValueArgs{
					ValueWrapper: value,
					ParseTime:    tc.parseTime,
				})

				assert.NoError(t, err, tc.colName)
				assert.Equal(t, tc.expectedValue, value.Value, tc.colName)
			}
		}
	}
}
