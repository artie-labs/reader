package converters

import (
	"fmt"

	transferDBZ "github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/lib/debezium"
)

type decimalConverter struct {
	scale     int
	precision *int
}

func NewDecimalConverter(scale int, precision *int) decimalConverter {
	return decimalConverter{scale: scale, precision: precision}
}

func (d decimalConverter) ToField(name string) transferDBZ.Field {
	field := transferDBZ.Field{
		FieldName:    name,
		DebeziumType: string(transferDBZ.KafkaDecimalType),
		Parameters: map[string]any{
			"scale": fmt.Sprint(d.scale),
		},
	}

	if d.precision != nil {
		field.Parameters[transferDBZ.KafkaDecimalPrecisionKey] = fmt.Sprint(*d.precision)
	}

	return field
}

func (d decimalConverter) Convert(value any) (any, error) {
	castValue, isOk := value.(string)
	if !isOk {
		return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
	}

	return debezium.EncodeDecimalToBase64(castValue, d.scale)
}

type VariableNumericConverter struct{}

func (VariableNumericConverter) ToField(name string) transferDBZ.Field {
	return transferDBZ.Field{
		FieldName:    name,
		Type:         "struct",
		DebeziumType: string(transferDBZ.KafkaVariableNumericType),
	}
}

func (VariableNumericConverter) Convert(value any) (any, error) {
	stringValue, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
	}

	scale := debezium.GetScale(stringValue)

	encodedValue, err := debezium.EncodeDecimalToBase64(stringValue, scale)
	if err != nil {
		return nil, fmt.Errorf("failed to encode decimal to b64: %w", err)
	}

	return map[string]string{
		"scale": fmt.Sprint(scale),
		"value": encodedValue,
	}, nil
}
