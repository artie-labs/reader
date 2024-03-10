package adapter

import (
	"fmt"
	"log/slog"
	"math"
	"time"

	transferDbz "github.com/artie-labs/transfer/lib/debezium"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/artie-labs/reader/lib/debezium"
	"github.com/artie-labs/reader/lib/debezium/converters"
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

// TODO: This converter doesn't check types, replace uses of this with specific converters from `debezium/converters`
type passthroughConverter struct {
	fieldType    string
	debeziumType string
}

func NewPassthroughConverter(fieldType, debeziumType string) converters.ValueConverter {
	return passthroughConverter{fieldType: fieldType, debeziumType: debeziumType}
}

func (p passthroughConverter) ToField(name string) transferDbz.Field {
	return transferDbz.Field{
		FieldName:    name,
		DebeziumType: p.debeziumType,
		Type:         p.fieldType,
	}
}

func (passthroughConverter) Convert(value any) (any, error) {
	return value, nil
}

type PgIntervalConverter struct{}

func (PgIntervalConverter) ToField(name string) transferDbz.Field {
	// The approximate number of microseconds for a time interval using the 365.25 / 12.0 formula for days per month average.
	return transferDbz.Field{
		FieldName:    name,
		Type:         "int64",
		DebeziumType: "io.debezium.time.MicroDuration",
	}
}

func (PgIntervalConverter) Convert(value any) (any, error) {
	intervalValue, ok := value.(pgtype.Interval)
	if !ok {
		return nil, fmt.Errorf("expected pgtype.Interval got %T with value: %v", value, value)
	}
	if !intervalValue.Valid {
		return nil, nil
	}

	totalDays := float64(intervalValue.Days) + float64(intervalValue.Months)*365.25/12.0
	microsecondsInDay := float64((time.Duration(24) * time.Hour) / time.Microsecond)
	if totalDays > math.MaxInt64/microsecondsInDay {
		return nil, fmt.Errorf("positive microseconds are too large for an int64")
	} else if totalDays < math.MinInt64/microsecondsInDay {
		return nil, fmt.Errorf("negative microseconds are too large for an int64")
	}
	daysInMicroseconds := int64(totalDays * microsecondsInDay)

	if daysInMicroseconds > 0 && intervalValue.Microseconds > 0 {
		if daysInMicroseconds > math.MaxInt64-intervalValue.Microseconds {
			return nil, fmt.Errorf("positive microseconds are too large for an int64")
		}
	} else if daysInMicroseconds < 0 && intervalValue.Microseconds < 0 {
		if daysInMicroseconds < math.MinInt64+intervalValue.Microseconds {
			return nil, fmt.Errorf("negative microseconds are too large for an int64")
		}
	}

	return intervalValue.Microseconds + daysInMicroseconds, nil
}
