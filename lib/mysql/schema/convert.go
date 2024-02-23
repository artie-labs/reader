package schema

import "fmt"

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
		if len(castValue) == 0 {
			return nil, fmt.Errorf("bit value is zero bytes: %v", value)
		}
		if castValue[0] == 0 {
			return false, nil
		} else if castValue[0] == 1 {
			return true, nil
		} else {
			return nil, fmt.Errorf("bit value is > 1: %v", value)
		}
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
	case Decimal,
		Date,
		DateTime,
		Time,
		Timestamp,
		Char,
		Varchar,
		Text,
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

	return nil, fmt.Errorf("could not convert DataType[%d] %T value: %v", colType, value, value)
}

// ConvertValues takes values returned from the MySQL driver and converts them to a native Go types.
func ConvertValues(values []any, cols []Column) ([]any, error) {
	if len(values) != len(cols) {
		return nil, fmt.Errorf("values and cols are not the same length")
	}

	result := make([]any, len(values))
	for idx, value := range values {
		col := cols[idx]
		convertedVal, err := ConvertValue(value, col.Type)
		if err != nil {
			return nil, fmt.Errorf("faild to convert value for column %s: %w", col.Name, err)
		}
		result[idx] = convertedVal
	}
	return result, nil
}
