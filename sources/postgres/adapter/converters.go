package adapter

import (
	"fmt"
	"log/slog"
	"time"

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

// TODO: Replace this with `converters.TimestampConverter` once we've run it for a while and not seen error logs
type PgTimestampConverter struct{}

func (PgTimestampConverter) ToField(name string) transferDbz.Field {
	return transferDbz.Field{
		FieldName:    name,
		DebeziumType: string(transferDbz.Timestamp),
		// NOTE: We are returning string here because we want the right layout to be used by our Typing library
		Type: "string",
	}
}

func (PgTimestampConverter) Convert(value any) (any, error) {
	valTime, isOk := value.(time.Time)
	if isOk {
		if valTime.Year() > 9999 || valTime.Year() < 0 {
			// Avoid copying this column over because it'll cause a JSON Marshal error:
			// Time.MarshalJSON: year outside of range [0,9999]
			slog.Info("Skipping timestamp because year is greater than 9999 or less than 0", slog.Any("value", value))
			return nil, nil
		}
	} else {
		slog.Error("Emitting a value for a timestamp column that is not a time.Time", slog.Any("value", value), slog.String("type", fmt.Sprintf("%T", value)))
	}

	return value, nil
}
