package antlr

import (
	"fmt"
	"github.com/artie-labs/reader/lib/antlr/generated"
)

func processDropTable(ctx *generated.DropTableContext) ([]Event, error) {
	var tableNames []string
	for _, child := range ctx.GetChildren() {
		if tableCtx, ok := child.(*generated.TablesContext); ok {
			for _, tableNameChild := range tableCtx.AllTableName() {
				tableName, err := getTableNameFromNode(tableNameChild)
				if err != nil {
					return nil, fmt.Errorf("failed to extract table name: %w", err)
				}

				tableNames = append(tableNames, tableName)
			}
		}
	}

	if len(tableNames) == 0 {
		return nil, fmt.Errorf("failed to extract table names")
	}

	var events []Event
	for _, tableName := range tableNames {
		events = append(events, DropTableEvent{TableName: tableName})
	}

	return events, nil
}
