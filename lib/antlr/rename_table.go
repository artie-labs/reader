package antlr

import (
	"fmt"
	"github.com/artie-labs/reader/lib/antlr/generated"
	"slices"
)

func processRenameTable(ctx *generated.RenameTableContext) ([]Event, error) {
	var renameEvents []Event
	for _, child := range ctx.GetChildren() {
		switch castedChild := child.(type) {
		case *generated.RenameTableClauseContext:
			var allTableNames []string
			for _, tableName := range castedChild.AllTableName() {
				allTableNames = append(allTableNames, tableName.GetText())
			}

			// Must be at least two table names
			if len(allTableNames) < 2 {
				return nil, fmt.Errorf("expected at least 2 table names, got %d", len(allTableNames))
			}

			// Make sure it's divisible by 2
			if len(allTableNames)%2 != 0 {
				return nil, fmt.Errorf("unexpected number of table names: %d", len(allTableNames))
			}

			for group := range slices.Chunk(allTableNames, 2) {
				renameEvents = append(renameEvents, RenameTableEvent{
					tableName:    group[0],
					newTableName: group[1],
				})
			}
		}
	}

	return renameEvents, nil
}
