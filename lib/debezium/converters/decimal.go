package converters

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/debezium"
)

type decimalConverter struct {
	scale     int
	precision *int
}

func NewDecimalConverter(scale int, precision *int) decimalConverter {
	return decimalConverter{scale: scale, precision: precision}
}

func (d decimalConverter) ToField(name string) debezium.Field {
	field := debezium.Field{
		FieldName:    name,
		Type:         debezium.Bytes,
		DebeziumType: debezium.KafkaDecimalType,
		Parameters: map[string]any{
			"scale": fmt.Sprint(d.scale),
		},
	}

	if d.precision != nil {
		field.Parameters[debezium.KafkaDecimalPrecisionKey] = fmt.Sprint(*d.precision)
	}

	return field
}

func (d decimalConverter) Convert(value any) (any, error) {
	castValue, isOk := value.(string)
	if !isOk {
		return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
	}
	return debezium.EncodeDecimal(castValue, d.scale)
}

func getScale(value string) int {
	// Find the index of the decimal point
	i := strings.IndexRune(value, '.')

	if i == -1 {
		// No decimal point: scale is 0
		return 0
	}

	// The scale is the number of digits after the decimal point
	return len(value[i+1:])
}

type VariableNumericConverter struct{}

func (VariableNumericConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.Struct,
		DebeziumType: debezium.KafkaVariableNumericType,
	}
}

func (VariableNumericConverter) Convert(value any) (any, error) {
	stringValue, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
	}

	scale := getScale(stringValue)

	bytes, err := debezium.EncodeDecimal(stringValue, scale)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"scale": int32(scale),
		"value": bytes,
	}, nil
}
