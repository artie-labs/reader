package converters

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/debezium"
)

type decimalConverter struct {
	scale     *int
	precision *int
}

func NewDecimalConverter(scale, precision *int) decimalConverter {
	return decimalConverter{scale: scale, precision: precision}
}

func (d decimalConverter) ToField(name string) debezium.Field {
	field := debezium.Field{
		FieldName:    name,
		DebeziumType: string(debezium.KafkaDecimalType),
	}

	if d.scale != nil && d.precision != nil {
		field.Parameters = make(map[string]any)

		if d.scale != nil {
			field.Parameters["scale"] = fmt.Sprint(*d.scale)
		}

		if d.precision != nil {
			field.Parameters[debezium.KafkaDecimalPrecisionKey] = fmt.Sprint(*d.precision)
		}
	}

	return field
}

func (decimalConverter) Convert(value any) (any, error) {
	castValue, isOk := value.(string)
	if isOk {
		return castValue, nil
	}

	return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
}
