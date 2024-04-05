package converters

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/artie-labs/transfer/lib/debezium"
)

func EncodeDecimalToBytes(value string, scale int) []byte {
	scaledValue := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	bigFloatValue := new(big.Float)
	bigFloatValue.SetString(value)
	bigFloatValue.Mul(bigFloatValue, new(big.Float).SetInt(scaledValue))

	// Extract the scaled integer value.
	bigIntValue, _ := bigFloatValue.Int(nil)
	data := bigIntValue.Bytes()
	if bigIntValue.Sign() < 0 {
		// Convert to two's complement if the number is negative
		bigIntValue = bigIntValue.Neg(bigIntValue)
		data = bigIntValue.Bytes()

		// Inverting bits for two's complement.
		for i := range data {
			data[i] = ^data[i]
		}

		// Adding one to complete two's complement.
		twoComplement := new(big.Int).SetBytes(data)
		twoComplement.Add(twoComplement, big.NewInt(1))

		data = twoComplement.Bytes()
		if data[0]&0x80 == 0 {
			// 0xff is -1 in Java
			// https://stackoverflow.com/questions/1677957/why-byte-b-byte-0xff-is-equals-to-integer-1
			data = append([]byte{0xff}, data...)
		}
	} else {
		// For positive values, prepend a zero if the highest bit is set to ensure it's interpreted as positive.
		if len(data) > 0 && data[0]&0x80 != 0 {
			data = append([]byte{0x00}, data...)
		}
	}
	return data
}

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
	return EncodeDecimalToBytes(castValue, d.scale), nil
}

func getScale(value string) int {
	// Find the index of the decimal point
	i := strings.IndexRune(value, '.')

	if i == -1 {
		// No decimal point: scale is 0
		return 0
	}

	// The scale is the number of digits after the decimal point
	scale := len(value[i+1:])

	return scale
}

type VariableNumericConverter struct{}

func (VariableNumericConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.Struct,
		DebeziumType: debezium.KafkaVariableNumericType,
	}
}

type VariableScaleDecimal struct {
	Scale int32  `json:"scale"`
	Value []byte `json:"value"`
}

func (VariableNumericConverter) Convert(value any) (any, error) {
	stringValue, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
	}

	scale := getScale(stringValue)
	return VariableScaleDecimal{
		Scale: int32(scale),
		Value: EncodeDecimalToBytes(stringValue, scale),
	}, nil
}
