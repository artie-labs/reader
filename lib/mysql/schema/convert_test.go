package schema

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func mustDecodeBase64(value string) []byte {
	result, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		panic(err)
	}
	return result
}

func TestConvertValue(t *testing.T) {
	tests := []struct {
		name        string
		dataType    DataType
		value       any
		expected    any
		expectedErr string
	}{
		{
			name:     "nil",
			dataType: Varchar,
			value:    nil,
			expected: nil,
		},
		{
			name:     "bit - 0 value",
			dataType: Bit,
			value:    []byte{byte(0)},
			expected: false,
		},
		{
			name:     "bit - 1 value",
			dataType: Bit,
			value:    []byte{byte(1)},
			expected: true,
		},
		{
			name:        "bit - 2 value",
			dataType:    Bit,
			value:       []byte{byte(2)},
			expectedErr: "bit value is invalid",
		},
		{
			name:        "bit - 2 bytes",
			dataType:    Bit,
			value:       []byte{byte(1), byte(1)},
			expectedErr: "bit value is invalid",
		},
		{
			name:     "boolean - 0",
			dataType: Boolean,
			value:    int64(0),
			expected: false,
		},
		{
			name:     "boolean - 1",
			dataType: Boolean,
			value:    int64(1),
			expected: true,
		},
		{
			name:        "boolean - -2",
			dataType:    Boolean,
			value:       int64(-2),
			expectedErr: "boolean value -2 not in [0, 1]",
		},
		{
			name:        "boolean - 2",
			dataType:    Boolean,
			value:       int64(2),
			expectedErr: "boolean value 2 not in [0, 1]",
		},
		{
			name:     "tiny int",
			dataType: TinyInt,
			value:    int64(100),
			expected: int64(100),
		},
		{
			name:        "tiny int - malformed",
			dataType:    TinyInt,
			value:       "bad tiny int",
			expectedErr: "expected int64 got string for value",
		},
		{
			name:     "small int",
			dataType: SmallInt,
			value:    int64(100),
			expected: int64(100),
		},
		{
			name:     "medium int",
			dataType: MediumInt,
			value:    int64(100),
			expected: int64(100),
		},
		{
			name:     "big int",
			dataType: BigInt,
			value:    int64(100),
			expected: int64(100),
		},
		{
			name:     "int",
			dataType: Int,
			value:    int64(100),
			expected: int64(100),
		},
		{
			name:     "year",
			dataType: Year,
			value:    int64(2021),
			expected: int64(2021),
		},
		{
			name:     "float",
			dataType: Float,
			value:    float32(1.234),
			expected: float32(1.234),
		},
		{
			name:        "float - malformed",
			dataType:    Float,
			value:       "bad float",
			expectedErr: "expected float32 got string for value",
		},
		{
			name:     "double",
			dataType: Double,
			value:    float64(1.234),
			expected: float64(1.234),
		},
		{
			name:     "binary",
			dataType: Binary,
			value:    []byte("hello world"),
			expected: []byte("hello world"),
		},
		{
			name:        "binary - malformed",
			dataType:    Binary,
			value:       "bad binary",
			expectedErr: "expected []byte got string for value",
		},
		{
			name:     "varbinary",
			dataType: Varbinary,
			value:    []byte("hello world"),
			expected: []byte("hello world"),
		},
		{
			name:     "blob",
			dataType: Blob,
			value:    []byte("hello world"),
			expected: []byte("hello world"),
		},
		{
			name:     "decimal",
			dataType: Decimal,
			value:    []byte("1.234"),
			expected: "1.234",
		},
		{
			name:     "date",
			dataType: Date,
			value:    []byte("2021-01-02"),
			expected: "2021-01-02",
		},
		{
			name:     "date - 0000-00-00",
			dataType: Date,
			value:    []byte("0000-00-00"),
			expected: "0000-00-00",
		},
		{
			name:     "datetime (0000-00-00)",
			dataType: DateTime,
			value:    []byte("0000-00-00 00:00:00"),
			expected: nil,
		},
		{
			name:     "datetime",
			dataType: DateTime,
			value:    []byte("2021-01-02 03:04:05"),
			expected: time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC),
		},
		{
			name:        "datetime - malformed",
			dataType:    DateTime,
			value:       []byte("not a datetime"),
			expectedErr: `cannot parse "not a datetime" as "2006"`,
		},
		{
			name:     "timestamp",
			dataType: Timestamp,
			value:    []byte("2021-01-02 03:04:05"),
			expected: time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC),
		},
		{
			name:        "timestamp - malformed",
			dataType:    Timestamp,
			value:       []byte("not a timestamp"),
			expectedErr: `cannot parse "not a timestamp" as "2006"`,
		},
		{
			name:     "time",
			dataType: Time,
			value:    []byte("10:10:10"),
			expected: "10:10:10",
		},
		{
			name:     "char",
			dataType: Char,
			value:    []byte("X"),
			expected: "X",
		},
		{
			name:     "varchar",
			dataType: Varchar,
			value:    []byte("hello world"),
			expected: "hello world",
		},
		{
			name:        "varchar - malformed",
			dataType:    Varchar,
			value:       1234,
			expectedErr: "expected []byte got int for value: 1234",
		},
		{
			name:     "text",
			dataType: Text,
			value:    []byte("hello world"),
			expected: "hello world",
		},
		{
			name:     "enum",
			dataType: Enum,
			value:    []byte("orange"),
			expected: "orange",
		},
		{
			name:     "set",
			dataType: Set,
			value:    []byte("orange"),
			expected: "orange",
		},
		{
			name:     "json",
			dataType: JSON,
			value:    []byte(`{"foo": "bar", "baz": "1234"}`),
			expected: `{"foo": "bar", "baz": "1234"}`,
		},
		{
			name:     "point - zero values",
			dataType: Point,
			value:    mustDecodeBase64("AAAAAAEBAAAAAAAAAAAAAAAAAAAAAAAAAA=="),
			expected: map[string]any{"x": 0.0, "y": 0.0},
		},
		{
			name:     "point - positive values",
			dataType: Point,
			value:    mustDecodeBase64("AAAAAAEBAAAArkfhehSuKECkcD0K12NMQA=="),
			expected: map[string]any{"x": 12.34, "y": 56.78},
		},
		{
			name:     "point - negative values",
			dataType: Point,
			value:    mustDecodeBase64("AAAAAAEBAAAASOF6FK5IocDD9ShcjzmqwA=="),
			expected: map[string]any{"x": -2212.34, "y": -3356.78},
		},
	}

	for _, tc := range tests {
		value, err := ConvertValue(tc.value, tc.dataType)
		if tc.expectedErr == "" {
			assert.NoError(t, err, tc.name)
			assert.Equal(t, tc.expected, value, tc.name)
		} else {
			assert.ErrorContains(t, err, tc.expectedErr, tc.name)
		}
	}
}

func TestConvertValues(t *testing.T) {
	columns := []Column{
		{Name: "a", Type: Int},
		{Name: "b", Type: Varchar},
		{Name: "c", Type: Bit},
	}

	{
		// Too few values
		assert.ErrorContains(t, ConvertValues([]any{}, columns), "values and cols are not the same length")
	}
	{
		// Malformed data
		err := ConvertValues([]any{"bad", "bad", "bad"}, columns)
		assert.ErrorContains(t, err, `failed to convert value for column "a": expected int64 got string for value: bad`)
	}
	{
		// Happy path - nils
		values := []any{nil, nil, nil}
		assert.NoError(t, ConvertValues(values, columns))
		assert.Equal(t, []any{nil, nil, nil}, values)
	}
	{
		// Happy path - no nils
		values := []any{int64(1234), []byte("hello world"), []byte{byte(1)}}
		assert.NoError(t, ConvertValues(values, columns))
		assert.Equal(t, []any{int64(1234), "hello world", true}, values)
	}
}

func TestHasNonStrictModeDate(t *testing.T) {
	assert.False(t, hasNonStrictModeDate(""))
	assert.False(t, hasNonStrictModeDate("hello world"))
	assert.False(t, hasNonStrictModeDate("2021-01-02"))
	assert.False(t, hasNonStrictModeDate("2021--01-02"))
	assert.False(t, hasNonStrictModeDate("2021-01-02 03:04:05"))

	assert.True(t, hasNonStrictModeDate("2009-00-00"))
	assert.True(t, hasNonStrictModeDate("0000-00-00"))
	assert.True(t, hasNonStrictModeDate("0000-00-00 00:00:00"))
	assert.True(t, hasNonStrictModeDate("2009-00-00 00:00:00"))
}
