package converters

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/debezium"
)

type MicroTimeConverter struct{}

func (MicroTimeConverter) ToField(name string) debezium.Field {
	// Represents the number of microseconds past midnight.
	return debezium.Field{
		FieldName:    name,
		Type:         "int64",
		DebeziumType: string(debezium.TimeMicro),
	}
}

func (MicroTimeConverter) Convert(value any) (any, error) {
	strValue, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string got %T with value: %v", value, value)
	}
	timeValue, err := time.Parse(time.TimeOnly, strValue)
	if err != nil {
		return nil, err
	}

	hours := time.Duration(timeValue.Hour()) * time.Hour
	minutes := time.Duration(timeValue.Minute()) * time.Minute
	seconds := time.Duration(timeValue.Second()) * time.Second
	return int64((hours + minutes + seconds) / time.Microsecond), nil
}

type DateConverter struct{}

func (DateConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         "int32",
		DebeziumType: string(debezium.Date),
	}
}

func (DateConverter) Convert(value any) (any, error) {
	ts, isOk := value.(time.Time)
	if !isOk {
		return nil, fmt.Errorf("object is not a time.Time object")
	}

	unix := time.UnixMilli(0).In(time.UTC) // 1970-01-01
	return int(ts.Sub(unix).Hours() / 24), nil
}

type TimestampConverter struct{}

func (TimestampConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         "string",
		DebeziumType: string(debezium.Timestamp),
	}
}

func (TimestampConverter) Convert(value any) (any, error) {
	timeValue, ok := value.(time.Time)
	if !ok {
		return nil, fmt.Errorf("expected time.Time got %T with value: %v", value, value)
	}

	if timeValue.Year() > 9999 || timeValue.Year() < 0 {
		// Avoid copying this column over because it'll cause a JSON Marshal error:
		// Time.MarshalJSON: year outside of range [0,9999]
		slog.Info("Skipping timestamp because year is greater than 9999 or less than 0", slog.Any("value", value))
		return nil, nil
	}

	return timeValue, nil
}

type YearConverter struct{}

func (YearConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         "int32",
		DebeziumType: "io.debezium.time.Year",
	}
}

func (YearConverter) Convert(value any) (any, error) {
	return asInt32(value)
}
