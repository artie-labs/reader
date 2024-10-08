package converters

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/debezium/converters"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/cockroachdb/apd/v3"
)

// encodeDecimalWithScale is used to encode a [*apd.Decimal] to `org.apache.kafka.connect.data.Decimal`
// using a specific scale.
func encodeDecimalWithScale(decimal *apd.Decimal, scale int32) ([]byte, error) {
	if decimal.Form != apd.Finite {
		return nil, fmt.Errorf("decimal (%v) is not finite", decimal)
	}

	targetExponent := -scale // Negate scale since [Decimal.Exponent] is negative.
	if decimal.Exponent != targetExponent {
		// Return an error if the scales are different, this maintains parity with `org.apache.kafka.connect.data.Decimal`.
		// https://github.com/a0x8o/kafka/blob/54eff6af115ee647f60129f2ce6a044cb17215d0/connect/api/src/main/java/org/apache/kafka/connect/data/Decimal.java#L69
		return nil, fmt.Errorf("value scale (%d) is different from schema scale (%d)", -decimal.Exponent, scale)
	}
	bytes, _ := converters.EncodeDecimal(decimal)
	return bytes, nil
}

type DecimalConverter struct {
	scale     uint16
	precision *int
}

func NewDecimalConverter(scale uint16, precision *int) DecimalConverter {
	return DecimalConverter{scale: scale, precision: precision}
}

func (d DecimalConverter) ToField(name string) debezium.Field {
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

func (d DecimalConverter) Convert(value any) (any, error) {
	stringValue, err := typing.AssertType[string](value)
	if err != nil {
		return nil, err
	}

	decimal, _, err := apd.NewFromString(stringValue)
	if err != nil {
		return nil, fmt.Errorf(`unable to use %q as a decimal: %w`, stringValue, err)
	}

	if decimal.Form == apd.NaN {
		return nil, nil
	}

	return encodeDecimalWithScale(decimal, int32(d.scale))
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
	stringValue, err := typing.AssertType[string](value)
	if err != nil {
		return nil, err
	}

	decimal, _, err := apd.NewFromString(stringValue)
	if err != nil {
		return nil, fmt.Errorf(`unable to use %q as a decimal: %w`, stringValue, err)
	}

	if decimal.Form == apd.NaN {
		return nil, nil
	}

	bytes, scale := converters.EncodeDecimal(decimal)
	return map[string]any{
		"scale": scale,
		"value": bytes,
	}, nil
}
