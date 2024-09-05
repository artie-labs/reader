package converters

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/debezium/converters"
	"log/slog"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/cockroachdb/apd/v3"
)

// decimalWithNewExponent takes a [*apd.Decimal] and returns a new [*apd.Decimal] with a the given exponent.
// If the new exponent is less precise then the extra digits will be truncated.
func decimalWithNewExponent(decimal *apd.Decimal, newExponent int32) *apd.Decimal {
	exponentDelta := newExponent - decimal.Exponent // Exponent is negative.

	if exponentDelta == 0 {
		return new(apd.Decimal).Set(decimal)
	}

	coefficient := new(apd.BigInt).Set(&decimal.Coeff)

	if exponentDelta < 0 {
		multiplier := new(apd.BigInt).Exp(apd.NewBigInt(10), apd.NewBigInt(int64(-exponentDelta)), nil)
		coefficient.Mul(coefficient, multiplier)
	} else if exponentDelta > 0 {
		divisor := new(apd.BigInt).Exp(apd.NewBigInt(10), apd.NewBigInt(int64(exponentDelta)), nil)
		coefficient.Div(coefficient, divisor)
	}

	return &apd.Decimal{
		Form:     decimal.Form,
		Negative: decimal.Negative,
		Exponent: newExponent,
		Coeff:    *coefficient,
	}
}

// encodeDecimalWithScale is used to encode a [*apd.Decimal] to `org.apache.kafka.connect.data.Decimal`
// using a specific scale.
func encodeDecimalWithScale(decimal *apd.Decimal, scale int32) []byte {
	targetExponent := -scale // Negate scale since [Decimal.Exponent] is negative.
	if decimal.Exponent != targetExponent {
		// TODO: We may be able to remove this conversion and just return an error to maintain parity with `org.apache.kafka.connect.data.Decimal`
		// https://github.com/a0x8o/kafka/blob/54eff6af115ee647f60129f2ce6a044cb17215d0/connect/api/src/main/java/org/apache/kafka/connect/data/Decimal.java#L69
		slog.Warn("Value scale is different from schema scale",
			slog.Any("value", decimal.Text('f')),
			slog.Any("actual", -decimal.Exponent),
			slog.Any("expected", scale),
		)

		decimal = decimalWithNewExponent(decimal, targetExponent)
	}
	bytes, _ := converters.EncodeDecimal(decimal)
	return bytes
}

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
	stringValue, err := typing.AssertType[string](value)
	if err != nil {
		return nil, err
	}

	decimal, _, err := apd.NewFromString(stringValue)
	if err != nil {
		return nil, fmt.Errorf(`unable to use %q as a decimal: %w`, stringValue, err)
	}

	return encodeDecimalWithScale(decimal, int32(d.scale)), nil
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

	bytes, scale := converters.EncodeDecimal(decimal)
	return map[string]any{
		"scale": scale,
		"value": bytes,
	}, nil
}
