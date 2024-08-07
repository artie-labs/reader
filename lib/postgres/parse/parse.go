package parse

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/artie-labs/reader/lib/postgres/schema"
)

func ParseValue(colKind schema.DataType, value any) (any, error) {
	// If the value is nil - just return.
	if value == nil {
		return nil, nil
	}

	switch colKind {
	case schema.Bit:
		valString, isOk := value.(string)
		if isOk {
			return valString, nil
		}
		return nil, fmt.Errorf("value: %v not of string type for bit", value)
	case schema.Real:
		float64Value, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("expected float64 got %T with value: %v", value, value)
		}
		// pgx returns `real`s as float64 even though they are always 32 bits
		// https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-NUMERIC
		return float32(float64Value), nil
	case schema.UserDefinedText:
		stringSlice, isOk := value.(string)
		if !isOk {
			return nil, fmt.Errorf("value: %v not of slice type", value)
		}

		return stringSlice, nil
	case schema.Numeric, schema.VariableNumeric:
		stringVal, isStringVal := value.(string)
		if isStringVal {
			return stringVal, nil
		}

		return nil, fmt.Errorf("value: %v not of string type for Numeric or VariableNumeric", value)
	case schema.TimeWithTimeZone:
		stringValue, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
		}

		return stringValue, nil
	case schema.Time:
		stringValue, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
		}
		var timeValue pgtype.Time
		if err := timeValue.Scan(stringValue); err != nil {
			return nil, fmt.Errorf("failed to parse time value %q: %w", stringValue, err)
		}
		if !timeValue.Valid {
			return nil, fmt.Errorf("parsed time value %q is not valid", stringValue)
		}
		return timeValue, nil
	case schema.Interval:
		stringValue, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
		}
		var intervalValue pgtype.Interval
		if err := intervalValue.Scan(stringValue); err != nil {
			return nil, fmt.Errorf("failed to parse interval value %q: %w", value, err)
		}
		if !intervalValue.Valid {
			return nil, nil
		}
		return intervalValue, nil
	case schema.Array:
		stringValue, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
		}

		var arr []any
		err := json.Unmarshal([]byte(stringValue), &arr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse array value %v: %w", value, err)
		}
		return arr, nil
	case schema.UUID:
		stringVal, isOk := value.(string)
		if !isOk {
			return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
		}

		_uuid, err := uuid.Parse(stringVal)
		if err != nil {
			return nil, fmt.Errorf("failed to cast uuid into *uuid.UUID: %w", err)
		}

		return _uuid.String(), nil
	case schema.JSON:
		byteSlice, isByteSlice := value.([]byte)
		if !isByteSlice {
			return nil, fmt.Errorf("value: %v not of []byte type for JSON", value)
		}

		return string(byteSlice), nil
	case schema.HStore:
		var val pgtype.Hstore
		err := val.Scan(value)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal hstore: %w", err)
		}

		jsonMap := make(map[string]string)
		for key, value := range val {
			if value != nil {
				jsonMap[key] = *value
			}
		}

		return jsonMap, nil
	case schema.Point:
		valString, isOk := value.(string)
		if !isOk {
			return nil, fmt.Errorf("value: %v not of string type for POINT", value)
		}

		point, err := ToPoint(valString)
		if err != nil {
			return nil, fmt.Errorf("failed to parse POINT: %w", err)
		}

		return point.ToMap(), nil
	case schema.Geometry, schema.Geography:
		valString, isOk := value.(string)
		if !isOk {
			return nil, fmt.Errorf("value: %v not of string type for geometry / geography", value)
		}

		geometry, err := ToGeography([]byte(valString))
		if err != nil {
			return nil, fmt.Errorf("failed to parse geometry / geography: %w", err)
		}

		return geometry, nil
	case schema.Money:
		valString, isOk := value.(string)
		if !isOk {
			return nil, fmt.Errorf("expected string, got: %T with value: %v for money", value, value)
		}

		return valString, nil
	default:
		return value, nil
	}
}
