package debezium

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"
)

type NewArgs struct {
	TableName string
	Fields    []debezium.Field
	RowData   map[string]interface{}
}

func (n *NewArgs) Validate() error {
	if n == nil {
		return fmt.Errorf("newArgs is nil")
	}

	if n.TableName == "" {
		return fmt.Errorf("tableName is empty")
	}

	if len(n.RowData) == 0 {
		return fmt.Errorf("rowData is empty")
	}

	if len(n.Fields) == 0 {
		return fmt.Errorf("fields is empty")
	}

	return nil
}

func NewPayload(newArgs *NewArgs) (util.SchemaEventPayload, error) {
	if err := newArgs.Validate(); err != nil {
		return util.SchemaEventPayload{}, fmt.Errorf("failed to validate: %w", err)
	}

	schema := debezium.Schema{
		FieldsObject: []debezium.FieldsObject{{
			Fields:     newArgs.Fields,
			Optional:   false,
			FieldLabel: cdc.After,
		}},
	}

	payload := util.Payload{
		After: newArgs.RowData,
		Source: util.Source{
			Table: newArgs.TableName,
			TsMs:  time.Now().UnixMilli(),
		},
		Operation: "r",
	}

	return util.SchemaEventPayload{
		Schema:  schema,
		Payload: payload,
	}, nil
}
