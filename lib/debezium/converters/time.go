package converters

import (
	"github.com/artie-labs/transfer/lib/debezium"
)

type MicroTimeConverter struct{}

func (MicroTimeConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         "int64",
		DebeziumType: string(debezium.TimeMicro),
	}
}

func (MicroTimeConverter) Convert(value any) (any, error) {
	panic("not implemented")
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
	panic("not implemented")
}

type TimestampConverter struct{}

func (TimestampConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         "int64",
		DebeziumType: string(debezium.Timestamp),
	}
}

func (TimestampConverter) Convert(value any) (any, error) {
	panic("not implemented")
}

type DateTimeWithTimezoneConverter struct{}

func (DateTimeWithTimezoneConverter) ToField(name string) debezium.Field {
	return debezium.Field{
		FieldName:    name,
		Type:         "string",
		DebeziumType: string(debezium.DateTimeWithTimezone),
	}
}

func (DateTimeWithTimezoneConverter) Convert(value any) (any, error) {
	panic("not implemented")
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
	panic("not implemented")
}
