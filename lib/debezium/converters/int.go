package converters

import (
	"fmt"
	"math"
)

func asInt16(value any) (int16, error) {
	switch castValue := value.(type) {
	case int16:
		return castValue, nil
	case int32:
		if castValue > math.MaxInt16 {
			return 0, fmt.Errorf("value is too large for int16")
		}
		return int16(castValue), nil
	case int:
		if castValue > math.MaxInt16 {
			return 0, fmt.Errorf("value is too large for int16")
		}
		return int16(castValue), nil
	case int64:
		if castValue > math.MaxInt16 {
			return 0, fmt.Errorf("value is too large for int16")
		}
		return int16(castValue), nil
	}
	return 0, fmt.Errorf("expected int/int16/int32/int64 got %T with value: %v", value, value)
}

func asInt32(value any) (int32, error) {
	switch castValue := value.(type) {
	case int16:
		return int32(castValue), nil
	case int32:
		return castValue, nil
	case int:
		if castValue > math.MaxInt32 {
			return 0, fmt.Errorf("value is too large for int32")
		}
		return int32(castValue), nil
	case int64:
		if castValue > math.MaxInt32 {
			return 0, fmt.Errorf("value is too large for int32")
		}
		return int32(castValue), nil
	}
	return 0, fmt.Errorf("expected int/int16/int32/int64 got %T with value: %v", value, value)
}

func asInt64(value any) (int64, error) {
	switch castValue := value.(type) {
	case int16:
		return int64(castValue), nil
	case int32:
		return int64(castValue), nil
	case int:
		return int64(castValue), nil
	case int64:
		return castValue, nil
	}
	return 0, fmt.Errorf("expected int/int16/int32/int64 got %T with value: %v", value, value)
}
