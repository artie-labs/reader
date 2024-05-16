package converters

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/debezium"
)

type MicroTimeConverter struct{}

func (MicroTimeConverter) ToField(name string) debezium.Field {
	// Represents the number of microseconds past midnight, and does not include timezone information.
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.Int64,
		DebeziumType: debezium.MicroTime,
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
		Type:         debezium.Int32,
		DebeziumType: debezium.Date,
	}
}

func (DateConverter) Convert(value any) (any, error) {
	var timeValue time.Time
	switch castValue := value.(type) {
	case time.Time:
		timeValue = castValue
	case string:
		if castValue == "0000-00-00" {
			// MySQL supports '0000-00-00' for date columns
			return nil, nil
		}
		var err error
		timeValue, err = time.Parse(time.DateOnly, castValue)
		if err != nil {
			return nil, fmt.Errorf("failed to convert to date: %w", err)
		}
	default:
		return nil, fmt.Errorf("expected string/time.Time got %T with value: %v", value, value)
	}

	return int32(timeValue.Unix() / (60 * 60 * 24)), nil
}

type MicroTimestampConverter struct{}

func (MicroTimestampConverter) ToField(name string) debezium.Field {
	// Represents the number of microseconds since the epoch, and does not include timezone information.
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.Int64,
		DebeziumType: debezium.MicroTimestamp,
	}
}

func (MicroTimestampConverter) Convert(value any) (any, error) {
	timeValue, ok := value.(time.Time)
	if !ok {
		return nil, fmt.Errorf("expected time.Time got %T with value: %v", value, value)
	}
	return timeValue.UnixMicro(), nil
}

type NanoTimestampConverter struct{}

//func (NanoTimestampConverter) ToField(name string) debezium.Field {
//	return debezium.Field{
//		FieldName:    name,
//		Type:         debezium.Int64,
//		DebeziumType: debezium.NanoTimestamp,
//	}
//}

type TimestampConverter struct{}

func (TimestampConverter) ToField(name string) debezium.Field {
	// Represents the number of milliseconds since the epoch, and does not include timezone information.
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.Int64,
		DebeziumType: debezium.Timestamp,
	}
}

type ZonedTimestampConverter struct{}

func (ZonedTimestampConverter) ToField(name string) debezium.Field {
	// A string representation of a timestamp with timezone information, where the timezone is GMT.
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.String,
		DebeziumType: debezium.DateTimeWithTimezone,
	}
}

func (ZonedTimestampConverter) Convert(value any) (any, error) {
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

	return timeValue.Format(time.RFC3339Nano), nil
}

type YearConverter struct{}

func (YearConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.Int32,
		DebeziumType: debezium.Year,
	}
}

func (YearConverter) Convert(value any) (any, error) {
	return asInt32(value)
}
