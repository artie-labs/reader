package converters

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/debezium"
)

// bool -> bool
type BooleanPassthrough struct{}

func (BooleanPassthrough) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName: name,
		Type:      debezium.Boolean,
	}
}

func (BooleanPassthrough) Convert(value any) (any, error) {
	boolVal, err := typing.AssertType[bool](value)
	if err != nil {
		return nil, err
	}

	return boolVal, nil
}

// int16, int32, int64 -> int16
type Int16Passthrough struct{}

func (Int16Passthrough) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName: name,
		Type:      debezium.Int16,
	}
}

func (Int16Passthrough) Convert(value any) (any, error) {
	return asInt16(value)
}

// int16, int32, int64 -> int32
type Int32Passthrough struct{}

func (Int32Passthrough) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName: name,
		Type:      debezium.Int32,
	}
}

func (Int32Passthrough) Convert(value any) (any, error) {
	return asInt32(value)
}

// int16, int32, int64 -> int64
type Int64Passthrough struct{}

func (Int64Passthrough) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName: name,
		Type:      debezium.Int64,
	}
}

func (Int64Passthrough) Convert(value any) (any, error) {
	return asInt64(value)
}

// float32, float64 -> float32
type FloatPassthrough struct{}

func (FloatPassthrough) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName: name,
		Type:      debezium.Float,
	}
}

func (FloatPassthrough) Convert(value any) (any, error) {
	switch castValue := value.(type) {
	case float32:
		return castValue, nil
	}
	return nil, fmt.Errorf("expected float32 got %T with value: %v", value, value)
}

// float32, float64 -> float64
type DoublePassthrough struct{}

func (DoublePassthrough) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName: name,
		Type:      debezium.Double,
	}
}

func (DoublePassthrough) Convert(value any) (any, error) {
	switch castValue := value.(type) {
	case float32:
		return float64(castValue), nil
	case float64:
		return castValue, nil
	}
	return nil, fmt.Errorf("expected float32/float64 got %T with value: %v", value, value)
}

// string -> string
type StringPassthrough struct{}

func (StringPassthrough) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName: name,
		Type:      debezium.String,
	}
}

func (StringPassthrough) Convert(value any) (any, error) {
	castedValue, err := typing.AssertType[string](value)
	if err != nil {
		return nil, err
	}

	return castedValue, nil
}

// bytes -> bytes
type BytesPassthrough struct{}

func (BytesPassthrough) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName: name,
		Type:      debezium.Bytes,
	}
}

func (BytesPassthrough) Convert(value any) (any, error) {
	castValue, isOk := value.([]byte)
	if isOk {
		return castValue, nil
	}
	return nil, fmt.Errorf("expected []byte got %T with value: %v", value, value)
}
