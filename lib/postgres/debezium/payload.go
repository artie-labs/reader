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
	Columns   []string
	Fields    *Fields
	RowData   map[string]interface{}
}

func (n *NewArgs) Validate() error {
	if n == nil {
		return fmt.Errorf("newArgs is nil")
	}

	if len(n.Columns) == 0 {
		return fmt.Errorf("columns is empty")
	}

	if n.TableName == "" {
		return fmt.Errorf("tableName is empty")
	}

	if len(n.RowData) == 0 {
		return fmt.Errorf("rowData is empty")
	}

	if n.Fields == nil {
		return fmt.Errorf("fields is nil")
	}

	return nil
}

func NewPayload(newArgs *NewArgs) (util.SchemaEventPayload, error) {
	if err := newArgs.Validate(); err != nil {
		return util.SchemaEventPayload{}, fmt.Errorf("failed to validate, err: %v", err)
	}

	schema := debezium.Schema{
		FieldsObject: []debezium.FieldsObject{{
			Fields:     newArgs.Fields.GetDebeziumFields(),
			Optional:   false,
			FieldLabel: cdc.After,
		}},
	}

	parsedRowData := make(map[string]interface{})
	for key, value := range newArgs.RowData {
		val, err := ParseValue(key, value, newArgs.Fields)
		if err != nil {
			return util.SchemaEventPayload{}, fmt.Errorf("failed to parseValue, err: %v", err)
		}

		parsedRowData[key] = val
	}

	payload := util.Payload{
		After: parsedRowData,
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
