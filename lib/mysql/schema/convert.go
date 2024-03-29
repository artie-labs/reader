package schema

import (
	"fmt"
	"time"
)

const DateTimeFormat = "2006-01-02 15:04:05.999999999"

// ConvertValue takes a value returned from the MySQL driver and converts it to a native Go type.
func ConvertValue(value any, colType DataType) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch colType {
	case Bit:
		// Bits
		castValue, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte got %T for value: %v", value, value)
		}
		if len(castValue) != 1 || castValue[0] > 1 {
			return nil, fmt.Errorf("bit value is invalid: %v", value)
		}
		return castValue[0] == 1, nil
	case Boolean:
		castVal, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("expected int64 got %T for value: %v", value, value)
		}
		if castVal > 1 || castVal < 0 {
			return nil, fmt.Errorf("boolean value not in [0, 1]: %v", value)
		}
		return castVal == 1, nil
	case TinyInt,
		SmallInt,
		MediumInt,
		Int,
		BigInt,
		Year:
		// Types we expect as int64
		_, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("expected int64 got %T for value: %v", value, value)
		}
		return value, nil
	case Float:
		// Types we expect as float32
		_, ok := value.(float32)
		if !ok {
			return nil, fmt.Errorf("expected float32 got %T for value: %v", value, value)
		}
		return value, nil
	case Double:
		// Types we expect as float64
		_, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("expected float64 got %T for value: %v", value, value)
		}
		return value, nil
	case Binary, Varbinary, Blob:
		// Types we expect as a byte array
		_, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte got %T for value: %v", value, value)
		}
		return value, nil
	case Date:
		bytesValue, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte got %T for value: %v", value, value)
		}

		// MySQL supports 0000-00-00 for dates so we can't use time.Time
		return string(bytesValue), nil
	case DateTime, Timestamp:
		bytesValue, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte got %T for value: %v", value, value)
		}
		timeValue, err := time.Parse(DateTimeFormat, string(bytesValue))
		if err != nil {
			return nil, err
		}
		return timeValue, nil
	case Decimal,
		Time,
		Char,
		Varchar,
		Text,
		TinyText,
		MediumText,
		LongText,
		Enum,
		Set,
		JSON:
		// Types that we expect as a byte array that will be converted to strings
		switch castValue := value.(type) {
		case []byte:
			return string(castValue), nil
		case string:
			// The driver should return these these types as []byte but no reason not to support strings too
			return castValue, nil
		default:
			return nil, fmt.Errorf("expected []byte got %T for value: %v", value, value)
		}
	}

	return nil, fmt.Errorf("could not convert DataType(%d) %T value: %v", colType, value, value)
}

// ConvertValues takes values returned from the MySQL driver and converts them to native Go types.
func ConvertValues(values []any, cols []Column) error {
	if len(values) != len(cols) {
		return fmt.Errorf("values and cols are not the same length")
	}

	for i, value := range values {
		col := cols[i]
		convertedVal, err := ConvertValue(value, col.Type)
		if err != nil {
			return fmt.Errorf("failed to convert value for column %s: %w", col.Name, err)
		}
		values[i] = convertedVal
	}
	return nil
}
