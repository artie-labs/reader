package adapter

import (
	"fmt"

	transferDbz "github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/lib/debezium"
	"github.com/artie-labs/reader/lib/stringutil"
)

const moneyScale = 2

type MoneyConverter struct{}

func (MoneyConverter) ToField(name string) transferDbz.Field {
	return transferDbz.Field{
		FieldName:    name,
		DebeziumType: string(transferDbz.KafkaDecimalType),
		Parameters: map[string]any{
			"scale": fmt.Sprint(moneyScale),
		},
	}
}

func (MoneyConverter) Convert(value any) (any, error) {
	stringValue := stringutil.ParseMoneyIntoString(fmt.Sprint(value))

	stringValue, err := debezium.EncodeDecimalToBase64(stringValue, moneyScale)
	if err != nil {
		return nil, fmt.Errorf("failed to encode decimal to b64: %w", err)
	}
	return stringValue, nil
}
