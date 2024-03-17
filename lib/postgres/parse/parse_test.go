package parse

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
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
			name:          "bit - 0",
			dataType:      schema.Bit,
			value:         "0",
			expectedValue: "0",
		},
		{
			name:          "bit - 1",
			dataType:      schema.Bit,
			value:         "1",
			expectedValue: "1",
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
			name:          "time - one second",
			dataType:      schema.Time,
			value:         "00:00:01",
			expectedValue: pgtype.Time{Microseconds: 100_0000, Valid: true},
		},
		{
			name:          "time - 24 hours",
			dataType:      schema.Time,
			value:         "24:00:00",
			expectedValue: pgtype.Time{Microseconds: 86_400_000_000, Valid: true},
		},
		{
			name:        "time - malformed",
			dataType:    schema.Time,
			value:       "blah",
			expectedErr: "failed to parse time value blah: cannot decode blah into Time",
		},
		{
			name:          "time with time zone - one second",
			dataType:      schema.TimeWithTimeZone,
			value:         "00:00:01",
			expectedValue: pgtype.Time{Microseconds: 100_0000, Valid: true},
		},
		{
			name:          "time with time zone  - 24 hours",
			dataType:      schema.TimeWithTimeZone,
			value:         "24:00:00",
			expectedValue: pgtype.Time{Microseconds: 86_400_000_000, Valid: true},
		},
		{
			name:        "time with time zone  - malformed",
			dataType:    schema.TimeWithTimeZone,
			value:       "blah",
			expectedErr: "failed to parse time value blah: cannot decode blah into Time",
		},
		{
			name:          "interval",
			dataType:      schema.Interval,
			value:         "1 day 2 mon 03:00:00",
			expectedValue: pgtype.Interval{Days: 1, Months: 2, Microseconds: 10_800_000_000, Valid: true},
		},
		{
			name:        "interval - malformed",
			dataType:    schema.Interval,
			value:       "blah",
			expectedErr: "failed to parse interval value blah: bad interval format",
		},
		{
			name:          "array - string",
			dataType:      schema.Array,
			value:         `["foo", "bar", "abc"]`,
			expectedValue: []any{"foo", "bar", "abc"},
		},
		{
			name:        "array - invalid type",
			dataType:    schema.Array,
			value:       1234,
			expectedErr: "expected string got int with value: 1234",
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
			name:        "uuid - not a string",
			dataType:    schema.UUID,
			value:       1234,
			expectedErr: "expected string got int with value: 1234",
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
			value:         `"foo"=>"bar", "baz"=>"qux"`,
			expectedValue: map[string]string{"foo": "bar", "baz": "qux"},
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
