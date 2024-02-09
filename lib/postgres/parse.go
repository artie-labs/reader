package postgres

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"

	"github.com/artie-labs/reader/lib/postgres/debezium"
	"github.com/artie-labs/reader/lib/postgres/parse"
	"github.com/artie-labs/reader/lib/timeutil"
)

type ParseValueArgs struct {
	ColName      string
	ParseTime    bool
	ValueWrapper ValueWrapper
}

func (p *ParseValueArgs) Value() interface{} {
	return p.ValueWrapper.Value
}

type ValueWrapper struct {
	Value  interface{}
	parsed bool
}

func (v *ValueWrapper) String() string {
	return fmt.Sprint(v.Value)
}

func NewValueWrapper(value interface{}) ValueWrapper {
	return ValueWrapper{
		Value:  value,
		parsed: true,
	}
}

func (c *Config) ParseValue(args ParseValueArgs) (ValueWrapper, error) {
	// If the value is nil, or already parsed - just return.
	if args.Value() == nil || args.ValueWrapper.parsed {
		return args.ValueWrapper, nil
	}

	colKind := c.Fields.GetDataType(args.ColName)
	switch colKind {
	case debezium.Geometry:
		valString, isOk := args.Value().(string)
		if !isOk {
			return NewValueWrapper(nil), fmt.Errorf("value: %v not of string type for geometry", args.Value())
		}

		geometry, err := parse.ToGeography([]byte(valString))
		if err != nil {
			return NewValueWrapper(nil), fmt.Errorf("failed to parse geometry: %w", err)
		}

		return NewValueWrapper(geometry), nil
	case debezium.Point:
		valString, isOk := args.Value().(string)
		if !isOk {
			return NewValueWrapper(nil), fmt.Errorf("value: %v not of string type for POINT", args.Value())
		}

		point, err := parse.ToPoint(valString)
		if err != nil {
			return NewValueWrapper(nil), fmt.Errorf("failed to parse POINT: %w", err)
		}

		return NewValueWrapper(point.ToMap()), nil

	case debezium.Bit:
		// This will be 0 (false) or 1 (true)
		valString, isOk := args.Value().(string)
		if isOk {
			return NewValueWrapper(valString == "1"), nil
		}
		return NewValueWrapper(nil), fmt.Errorf("value: %v not of string type for bit", args.Value())
	case debezium.JSON:
		// Debezium sends JSON as a JSON string
		byteSlice, isByteSlice := args.Value().([]byte)
		if !isByteSlice {
			return NewValueWrapper(nil), fmt.Errorf("value: %v not of []byte type for JSON", args.Value())
		}

		return NewValueWrapper(string(byteSlice)), nil
	case debezium.Numeric, debezium.VariableNumeric:
		stringVal, isStringVal := args.Value().(string)
		if isStringVal {
			return NewValueWrapper(stringVal), nil
		}

		return NewValueWrapper(nil), fmt.Errorf("value: %v not of string type for Numeric or VariableNumeric", args.Value())
	case debezium.Array:
		var arr []interface{}
		if reflect.TypeOf(args.Value()).Kind() == reflect.Slice {
			// If it's already a slice, don't modify it further.
			return NewValueWrapper(args.Value()), nil
		}

		err := json.Unmarshal([]byte(fmt.Sprint(args.Value())), &arr)
		if err != nil {
			return NewValueWrapper(nil), fmt.Errorf("failed to parse colName: %s, value: %v, err: %w", args.ColName, args.Value(), err)
		}
		return NewValueWrapper(arr), nil
	case debezium.UUID:
		stringVal, isOk := args.Value().(string)
		if !isOk {
			return NewValueWrapper(nil), fmt.Errorf("value: %v not of string type", args.Value())
		}

		_uuid, err := uuid.Parse(stringVal)
		if err != nil {
			return NewValueWrapper(nil), fmt.Errorf("failed to cast uuid into *uuid.UUID: %w", err)
		}

		return NewValueWrapper(_uuid.String()), nil
	case debezium.HStore:
		var val pgtype.Hstore
		err := val.Scan(args.Value())
		if err != nil {
			return NewValueWrapper(nil), fmt.Errorf("failed to unmarshal hstore: %w", err)
		}

		jsonMap := make(map[string]interface{})
		for key, value := range val.Map {
			if value.Status == pgtype.Present {
				jsonMap[key] = value.String
			}
		}

		return NewValueWrapper(jsonMap), nil
	case debezium.UserDefinedText:
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
