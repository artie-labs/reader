package converters

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/debezium"
)

type TimeConverter struct{}

func (TimeConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.Int32,
		DebeziumType: debezium.Time,
	}
}

func (TimeConverter) Convert(value any) (any, error) {
	switch timeValue := value.(type) {
	case time.Time:
		return int32(getTimeDuration(timeValue, time.Millisecond)), nil
	default:
		return nil, fmt.Errorf("expected time.Time got %T with value: %v", value, value)
	}
}

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
	var timeValue time.Time
	switch castedValue := value.(type) {
	case time.Time:
		timeValue = castedValue
	case string:
		var err error
		timeValue, err = time.Parse(time.TimeOnly, castedValue)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("expected string/time.Time got %T with value: %v", value, value)
	}

	return getTimeDuration(timeValue, time.Microsecond), nil
}

type NanoTimeConverter struct{}

func (NanoTimeConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.Int64,
		DebeziumType: debezium.NanoTime,
	}
}

func (NanoTimeConverter) Convert(value any) (any, error) {
	timeValue, ok := value.(time.Time)
	if !ok {
		return nil, fmt.Errorf("expected time.Time got %T with value: %v", value, value)
	}

	return getTimeDuration(timeValue, time.Nanosecond), nil
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
		parts := strings.Split(castValue, "-")
		if len(parts) == 3 {
			for _, part := range parts {
				castedPart, err := strconv.ParseInt(part, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("failed to parse date %q: %w", castValue, err)
				}

				if castedPart <= 0 {
					slog.Warn(fmt.Sprintf("Skipping invalid value: %q", castValue))
					// MySQL supports '0000-00-00' for date columns if strict mode is not enabled.
					return nil, nil
				}
			}
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

type TimestampConverter struct{}

func (TimestampConverter) ToField(name string) debezium.Field {
	// Represents the number of milliseconds since the epoch, and does not include timezone information.
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.Int64,
		DebeziumType: debezium.Timestamp,
	}
}

func (TimestampConverter) Convert(value any) (any, error) {
	timeValue, ok := value.(time.Time)
	if !ok {
		return nil, fmt.Errorf("expected time.Time got %T with value: %v", value, value)
	}

	return timeValue.UnixMilli(), nil
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

func (NanoTimestampConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.Int64,
		DebeziumType: debezium.NanoTimestamp,
	}
}

func (NanoTimestampConverter) Convert(value any) (any, error) {
	timeValue, ok := value.(time.Time)
	if !ok {
		return nil, fmt.Errorf("expected time.Time got %T with value: %v", value, value)
	}
	return timeValue.UnixMicro() * 1_000, nil
}

type ZonedTimestampConverter struct{}

func (ZonedTimestampConverter) ToField(name string) debezium.Field {
	// A string representation of a timestamp with timezone information, where the timezone is GMT.
	return debezium.Field{
		FieldName:    name,
		Type:         debezium.String,
		DebeziumType: debezium.ZonedTimestamp,
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

	// A string representation of a timestamp with timezone information, where the timezone is GMT.
	// This layout supports upto microsecond precision.
	layout := "2006-01-02T15:04:05.999999Z"
	return timeValue.UTC().Format(layout), nil
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
