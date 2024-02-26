package converters

import "github.com/artie-labs/transfer/lib/debezium"

type ValueConverter interface {
	ToField(name string) debezium.Field
	Convert(value any) (any, error)
}
