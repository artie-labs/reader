package converters

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/cockroachdb/apd/v3"
)

const defaultScale = uint16(2)

type MoneyConverter struct {
	// All of these configs are optional

	StripCommas    bool
	CurrencySymbol string
	ScaleOverride  *uint16
}

func (m MoneyConverter) Scale() uint16 {
	if m.ScaleOverride != nil {
		return *m.ScaleOverride
	}

	return defaultScale
}

func (m MoneyConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.Bytes,
		DebeziumType: debezium.KafkaDecimalType,
		Parameters: map[string]any{
			"scale": fmt.Sprint(m.Scale()),
		},
	}
}

func (m MoneyConverter) Convert(value any) (any, error) {
	valString, isOk := value.(string)
	if !isOk {
		return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
	}

	if m.CurrencySymbol != "" {
		valString = strings.Trim(valString, m.CurrencySymbol)
	}

	if m.StripCommas {
		valString = strings.ReplaceAll(valString, ",", "")
	}

	decimal, _, err := apd.NewFromString(valString)
	if err != nil {
		return nil, fmt.Errorf(`unable to use %q as a money value: %w`, valString, err)
	}

	return encodeDecimalWithScale(decimal, int32(m.Scale()))
}
