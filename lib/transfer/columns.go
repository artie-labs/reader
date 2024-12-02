package transfer

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/reader/lib/debezium/transformer"
)

type Adapter interface {
	FieldConverters() []transformer.FieldConverter
	PartitionKeys() []string
}

func BuildTransferColumns(adapter Adapter) ([]columns.Column, error) {
	var cols columns.Columns
	for _, fc := range adapter.FieldConverters() {
		kd, err := fc.ValueConverter.ToField(fc.Name).ToKindDetails()
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %q to kind details: %w", fc.Name, err)
		}

		cols.AddColumn(columns.NewColumn(fc.Name, kd))
	}

	for _, pk := range adapter.PartitionKeys() {
		err := cols.UpsertColumn(pk, columns.UpsertColumnArg{
			PrimaryKey: typing.ToPtr(true),
		})

		if err != nil {
			return nil, fmt.Errorf("failed to upsert primary key column %q: %w", pk, err)
		}
	}

	return cols.GetColumns(), nil
}
