package debezium

import (
	"math/big"
	"strings"
)

func GetScale(value string) int {
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
