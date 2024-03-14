package postgres

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
)

func TestConvertToQueryValue(t *testing.T) {
	testCases := []struct {
		name        string
		value       any
		expected    any
		expectedErr string
	}{
		{
			name:        "unsupported data type",
			value:       []byte("foo"),
			expectedErr: "unexpected type []uint8 for primary key with value ",
		},
		{
			name:     "nil",
			value:    nil,
			expected: nil,
		},
		{
			name:     "boolean - true",
			value:    true,
			expected: true,
		},
		{
			name:     "boolean - false",
			value:    false,
			expected: false,
		},
		{
			name:     "int",
			value:    int(1234),
			expected: int(1234),
		},
		{
			name:     "int8",
			value:    int8(12),
			expected: int8(12),
		},
		{
			name:     "int16",
			value:    int16(1234),
			expected: int16(1234),
		},
		{
			name:     "int32",
			value:    int32(1234),
			expected: int32(1234),
		},
		{
			name:     "int64",
			value:    int64(1234),
			expected: int64(1234),
		},
		{
			name:     "float32",
			value:    float32(1234.1234),
			expected: float32(1234.1234),
		},
		{
			name:     "float64",
			value:    float64(1234.1234),
			expected: float64(1234.1234),
		},
		{
			name:     "text",
			value:    "foo",
			expected: "foo",
		},
		{
			name:     "time",
			value:    time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC),
			expected: "2001-02-03T04:05:06Z",
		},
		{
			name:     "pgtype.Time - valid",
			value:    pgtype.Time{Microseconds: 1_000_000 * 30, Valid: true},
			expected: pgtype.Time{Microseconds: 1_000_000 * 30, Valid: true},
		},
		{
			name:     "pgtype.Interval - valid",
			value:    pgtype.Interval{Days: 2, Months: 1, Microseconds: 1_000_000, Valid: true},
			expected: pgtype.Interval{Days: 2, Months: 1, Microseconds: 1_000_000, Valid: true},
		},
	}
	for _, testCase := range testCases {
		actual, err := convertToQueryValue(testCase.value)
		if testCase.expectedErr == "" {
			assert.NoError(t, err)
			assert.Equal(t, testCase.expected, actual, testCase.name)
		} else {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
		}
	}
}

func TestScanTableQuery(t *testing.T) {
	primaryKeys := []primary_key.Key{
		{Name: "a", StartingValue: int64(1), EndingValue: int64(4)},
		{Name: "b", StartingValue: int64(2), EndingValue: int64(5)},
		{Name: "c", StartingValue: "3", EndingValue: "6"},
	}
	cols := []schema.Column{
		{Name: "a", Type: schema.Int64},
		{Name: "b", Type: schema.Int64},
		{Name: "c", Type: schema.Text},
		{Name: "e", Type: schema.Text},
		{Name: "f", Type: schema.Int64},
		{Name: "127.0.0.1", Type: schema.Inet},
	}

	{
		// inclusive lower bound
		query, parameters, err := scanTableQuery(scanTableQueryArgs{
			Schema:              "schema",
			TableName:           "table",
			PrimaryKeys:         primaryKeys,
			InclusiveLowerBound: true,
			Limit:               1,
			Columns:             cols,
		})
		assert.NoError(t, err)
		assert.Equal(t, `SELECT "a","b","c","e","f","127.0.0.1"::text FROM "schema"."table" WHERE row("a","b","c") >= row($1,$2,$3) AND row("a","b","c") <= row($4,$5,$6) ORDER BY "a","b","c" LIMIT 1`, query)
		assert.Equal(t, []any{int64(1), int64(2), "3", int64(4), int64(5), "6"}, parameters)
	}
	{
		// exclusive lower bound
		query, parameters, err := scanTableQuery(scanTableQueryArgs{
			Schema:              "schema",
			TableName:           "table",
			PrimaryKeys:         primaryKeys,
			InclusiveLowerBound: false,
			Limit:               2,
			Columns:             cols,
		})
		assert.NoError(t, err)
		assert.Equal(t, `SELECT "a","b","c","e","f","127.0.0.1"::text FROM "schema"."table" WHERE row("a","b","c") > row($1,$2,$3) AND row("a","b","c") <= row($4,$5,$6) ORDER BY "a","b","c" LIMIT 2`, query)
		assert.Equal(t, []any{int64(1), int64(2), "3", int64(4), int64(5), "6"}, parameters)
	}
}

func TestScanAdapter_ParsePrimaryKeyValue(t *testing.T) {
	{
		// Column does not exist
		adapter := scanAdapter{columns: []schema.Column{{Name: "bar"}}}
		_, err := adapter.ParsePrimaryKeyValue("foo", "1234")
		assert.ErrorContains(t, err, "primary key column does not exist: foo")
	}

	testCases := []struct {
		name        string
		dataType    schema.DataType
		value       string
		expected    any
		expectedErr string
	}{
		{
			name:        "unsupported data type",
			dataType:    schema.Array,
			value:       "1234",
			expectedErr: "DataType(21) for column 'col' is not supported for use as a primary key",
		},
		{
			name:        "boolean - malformed",
			dataType:    schema.Boolean,
			value:       "1234",
			expectedErr: `unable to convert "1234" to a bool`,
		},
		{
			name:     "boolean - valid",
			dataType: schema.Boolean,
			value:    "true",
			expected: true,
		},
		{
			name:        "int16 - malformed",
			dataType:    schema.Int16,
			value:       "true",
			expectedErr: `unable to convert "true" to an int16`,
		},
		{
			name:     "int16 - valid",
			dataType: schema.Int16,
			value:    "3412",
			expected: int16(3412),
		},
		{
			name:        "int32 - malformed",
			dataType:    schema.Int32,
			value:       "apple",
			expectedErr: `unable to convert "apple" to an int32`,
		},
		{
			name:     "int32 - valid",
			dataType: schema.Int32,
			value:    "32768",
			expected: int32(32768),
		},
		{
			name:        "int64 - malformed",
			dataType:    schema.Int64,
			value:       "orange",
			expectedErr: `unable to convert "orange" to an int64`,
		},
		{
			name:     "int64 - valid",
			dataType: schema.Int64,
			value:    "2147483647",
			expected: int64(2_147_483_647),
		},
		{
			name:        "real - malformed",
			dataType:    schema.Real,
			value:       "orange",
			expectedErr: `unable to convert "orange" to a float32`,
		},
		{
			name:     "real - valid",
			dataType: schema.Real,
			value:    "123445.79",
			expected: float32(12_3445.79),
		},
		{
			name:        "double - malformed",
			dataType:    schema.Double,
			value:       "orange",
			expectedErr: `unable to convert "orange" to a float64`,
		},
		{
			name:     "double - valid",
			dataType: schema.Double,
			value:    "3.141592653589793",
			expected: float64(3.141592653589793),
		},
		{
			name:     "text - valid",
			dataType: schema.Text,
			value:    "one two three four",
			expected: "one two three four",
		},
	}

	for _, testCase := range testCases {
		adapter := scanAdapter{columns: []schema.Column{{Name: "col", Type: testCase.dataType}}}
		value, err := adapter.ParsePrimaryKeyValue("col", testCase.value)
		if testCase.expectedErr == "" {
			assert.NoError(t, err, testCase.name)
			assert.Equal(t, testCase.expected, value, testCase.name)
		} else {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
		}
	}
}
