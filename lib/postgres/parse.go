package postgres

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"

	"github.com/artie-labs/reader/lib/postgres/parse"
	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/reader/lib/timeutil"
)

type ParseValueArgs struct {
	ParseTime    bool
	ValueWrapper ValueWrapper
}

func (p *ParseValueArgs) Value() any {
	return p.ValueWrapper.Value
}

type ValueWrapper struct {
	Value  any
	parsed bool
}

func NewValueWrapper(value any) ValueWrapper {
	return ValueWrapper{
		Value:  value,
		parsed: true,
	}
}

func ParseValue(colKind schema.DataType, args ParseValueArgs) (ValueWrapper, error) {
	// If the value is nil, or already parsed - just return.
	if args.Value() == nil || args.ValueWrapper.parsed {
		return args.ValueWrapper, nil
	}

	switch colKind {
	case schema.Geometry, schema.Geography:
		valString, isOk := args.Value().(string)
		if !isOk {
			return NewValueWrapper(nil), fmt.Errorf("value: %v not of string type for geometry / geography", args.Value())
		}

		geometry, err := parse.ToGeography([]byte(valString))
		if err != nil {
			return NewValueWrapper(nil), fmt.Errorf("failed to parse geometry / geography: %w", err)
		}

		return NewValueWrapper(geometry), nil
	case schema.Point:
		valString, isOk := args.Value().(string)
		if !isOk {
			return NewValueWrapper(nil), fmt.Errorf("value: %v not of string type for POINT", args.Value())
		}

		point, err := parse.ToPoint(valString)
		if err != nil {
			return NewValueWrapper(nil), fmt.Errorf("failed to parse POINT: %w", err)
		}

		return NewValueWrapper(point.ToMap()), nil

	case schema.Bit:
		// This will be 0 (false) or 1 (true)
		valString, isOk := args.Value().(string)
		if isOk {
			return NewValueWrapper(valString == "1"), nil
		}
		return NewValueWrapper(nil), fmt.Errorf("value: %v not of string type for bit", args.Value())
	case schema.JSON:
		// Debezium sends JSON as a JSON string
		byteSlice, isByteSlice := args.Value().([]byte)
		if !isByteSlice {
			return NewValueWrapper(nil), fmt.Errorf("value: %v not of []byte type for JSON", args.Value())
		}

		return NewValueWrapper(string(byteSlice)), nil
	case schema.Numeric, schema.VariableNumeric:
		stringVal, isStringVal := args.Value().(string)
		if isStringVal {
			return NewValueWrapper(stringVal), nil
		}

		return NewValueWrapper(nil), fmt.Errorf("value: %v not of string type for Numeric or VariableNumeric", args.Value())
	case schema.Array:
		var arr []any
		if reflect.TypeOf(args.Value()).Kind() == reflect.Slice {
			// If it's already a slice, don't modify it further.
			return NewValueWrapper(args.Value()), nil
		}

		err := json.Unmarshal([]byte(fmt.Sprint(args.Value())), &arr)
		if err != nil {
			return NewValueWrapper(nil), fmt.Errorf("failed to parse array value %v: %w", args.Value(), err)
		}
		return NewValueWrapper(arr), nil
	case schema.UUID:
		stringVal, isOk := args.Value().(string)
		if !isOk {
			return NewValueWrapper(nil), fmt.Errorf("value: %v not of string type", args.Value())
		}

		_uuid, err := uuid.Parse(stringVal)
		if err != nil {
			return NewValueWrapper(nil), fmt.Errorf("failed to cast uuid into *uuid.UUID: %w", err)
		}

		return NewValueWrapper(_uuid.String()), nil
	case schema.HStore:
		var val pgtype.Hstore
		err := val.Scan(args.Value())
		if err != nil {
			return NewValueWrapper(nil), fmt.Errorf("failed to unmarshal hstore: %w", err)
		}

		jsonMap := make(map[string]any)
		for key, value := range val.Map {
			if value.Status == pgtype.Present {
				jsonMap[key] = value.String
			}
		}

		return NewValueWrapper(jsonMap), nil
	case schema.UserDefinedText:
		stringSlice, isOk := args.Value().(string)
		if !isOk {
			return NewValueWrapper(nil), fmt.Errorf("value: %v not of slice type", args.Value())
		}

		return NewValueWrapper(stringSlice), nil
	default:
		// This is needed because we need to cast the time.Time object into a string for pagination.
		if args.ParseTime {
			return NewValueWrapper(timeutil.ParseValue(args.Value())), nil
		}

		// We don't care about anything other than arrays.
		// Return parsed = false since we didn't actually parse it.
		return ValueWrapper{
			Value:  args.Value(),
			parsed: false,
		}, nil
	}
}
