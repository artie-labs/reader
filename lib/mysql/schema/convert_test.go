package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
			name:     "datetime",
			dataType: DateTime,
			value:    []byte("2021-01-02 10:10:10"),
			expected: "2021-01-02 10:10:10",
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
		_, err := ConvertValues([]any{}, columns)
		assert.ErrorContains(t, err, "values and cols are not the same length")
	}
	{
		// Malformed data
		_, err := ConvertValues([]any{"bad", "bad", "bad"}, columns)
		assert.ErrorContains(t, err, "faild to convert value for column a: expected int64 got string for value: bad")
	}
	{
		// Happy path - nils
		result, err := ConvertValues([]any{nil, nil, nil}, columns)
		assert.NoError(t, err)
		assert.Equal(t, []any{nil, nil, nil}, result)
	}
	{
		// Happy path - no nils
		result, err := ConvertValues([]any{int64(1234), []byte("hello world"), []byte{byte(1)}}, columns)
		assert.NoError(t, err)
		assert.Equal(t, []any{int64(1234), "hello world", true}, result)
	}
}
