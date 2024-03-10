package parse

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/postgres/schema"
)

func TestParse(t *testing.T) {
	type _testCase struct {
		name          string
		dataType      schema.DataType
		value         any
		expectedErr   string
		expectedValue any
	}

	tcs := []_testCase{
		{
			name:          "bit - true",
			dataType:      schema.Bit,
			value:         "1",
			expectedValue: true,
		},
		{
			name:          "bit - false",
			dataType:      schema.Bit,
			value:         "0",
			expectedValue: false,
		},
		{
			name:        "bit - malformed",
			dataType:    schema.Bit,
			value:       1234,
			expectedErr: "value: 1234 not of string type for bit",
		},
		{
			name:          "text",
			dataType:      schema.Text,
			value:         "hello",
			expectedValue: "hello",
		},
		{
			name:          "array - string",
			dataType:      schema.Array,
			value:         `["foo", "bar", "abc"]`,
			expectedValue: []any{"foo", "bar", "abc"},
		},
		{
			name:          "array - bytes",
			dataType:      schema.Array,
			value:         []byte(`["foo", "bar", "abc"]`),
			expectedValue: []any{"foo", "bar", "abc"},
		},
		{
			name:          "array - slice",
			dataType:      schema.Array,
			value:         []any{"foo", "bar", "abc"},
			expectedValue: []any{"foo", "bar", "abc"},
		},
		{
			name:        "array - invalid type",
			dataType:    schema.Array,
			value:       1234,
			expectedErr: "expected array/string/[]byte got int with value: 1234",
		},
		{
			name:        "array - malformed",
			dataType:    schema.Array,
			value:       "1234",
			expectedErr: "failed to parse array value 1234:",
		},
		{
			name:          "uuid",
			dataType:      schema.UUID,
			value:         "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
			expectedValue: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
		},
		{
			name:        "uuid - malformed",
			dataType:    schema.UUID,
			value:       "abcd :(",
			expectedErr: "failed to cast uuid into *uuid.UUID",
		},
		{
			name:          "json",
			dataType:      schema.JSON,
			value:         []byte(`{"foo":"bar"}`),
			expectedValue: `{"foo":"bar"}`,
		},
		{
			name:          "hstore",
			dataType:      schema.HStore,
			value:         `"foo"=>"bar"`,
			expectedValue: map[string]any{"foo": "bar"},
		},
		{
			name:     "geography",
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
		if tc.expectedErr == "" {
			assert.NoError(t, err, tc.name)
			assert.Equal(t, tc.expectedValue, value, tc.name)
		} else {
			assert.ErrorContains(t, err, tc.expectedErr, tc.name)
		}
	}
}
