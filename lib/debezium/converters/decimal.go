package converters

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/cockroachdb/apd/v3"
)

type decimalConverter struct {
	scale     uint16
	precision *int
}

func NewDecimalConverter(scale uint16, precision *int) decimalConverter {
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
	stringValue, isOk := value.(string)
	if !isOk {
		return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
	}

	decimal, _, err := apd.NewFromString(stringValue)
	if err != nil {
		return nil, fmt.Errorf(`unable to use %q as a decimal: %w`, stringValue, err)
	}

	return debezium.EncodeDecimalWithScale(decimal, int32(d.scale)), nil
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

	decimal, _, err := apd.NewFromString(stringValue)
	if err != nil {
		return nil, fmt.Errorf(`unable to use %q as a decimal: %w`, stringValue, err)
	}

	bytes, scale := debezium.EncodeDecimal(decimal)
	return map[string]any{
		"scale": scale,
		"value": bytes,
	}, nil
}
