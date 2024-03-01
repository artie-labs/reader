package parse

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"

	"github.com/artie-labs/reader/lib/postgres/schema"
)

func ParseValue(colKind schema.DataType, value any) (any, error) {
	// If the value is nil - just return.
	if value == nil {
		return nil, nil
	}

	switch colKind {
	case schema.InvalidDataType:
		return nil, fmt.Errorf("invalid data type")
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

	case schema.Bit:
		// This will be 0 (false) or 1 (true)
		valString, isOk := value.(string)
		if isOk {
			return valString == "1", nil
		}
		return nil, fmt.Errorf("value: %v not of string type for bit", value)
	case schema.JSON:
		// Debezium sends JSON as a JSON string
		byteSlice, isByteSlice := value.([]byte)
		if !isByteSlice {
			return nil, fmt.Errorf("value: %v not of []byte type for JSON", value)
		}

		return string(byteSlice), nil
	case schema.Numeric, schema.VariableNumeric:
		stringVal, isStringVal := value.(string)
		if isStringVal {
			return stringVal, nil
		}

		return nil, fmt.Errorf("value: %v not of string type for Numeric or VariableNumeric", value)
	case schema.Array:
		var arr []any
		if reflect.TypeOf(value).Kind() == reflect.Slice {
			// If it's already a slice, don't modify it further.
			return value, nil
		}

		err := json.Unmarshal([]byte(fmt.Sprint(value)), &arr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse array value %v: %w", value, err)
		}
		return arr, nil
	case schema.UUID:
		stringVal, isOk := value.(string)
		if !isOk {
			return nil, fmt.Errorf("value: %v not of string type", value)
		}

		_uuid, err := uuid.Parse(stringVal)
		if err != nil {
			return nil, fmt.Errorf("failed to cast uuid into *uuid.UUID: %w", err)
		}

		return _uuid.String(), nil
	case schema.HStore:
		var val pgtype.Hstore
		err := val.Scan(value)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal hstore: %w", err)
		}

		jsonMap := make(map[string]any)
		for key, value := range val.Map {
			if value.Status == pgtype.Present {
				jsonMap[key] = value.String
			}
		}

		return jsonMap, nil
	case schema.UserDefinedText:
		stringSlice, isOk := value.(string)
		if !isOk {
			return nil, fmt.Errorf("value: %v not of slice type", value)
		}

		return stringSlice, nil
	default:
		return value, nil
	}
}
