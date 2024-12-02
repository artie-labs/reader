package antlr

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/artie-labs/reader/lib/antlr/generated"
)

func processAlterTable(ctx *generated.AlterTableContext) ([]Event, error) {
	var events []Event
	tableName, err := getTableNameFromNode(ctx.TableName())
	if err != nil {
		return nil, err
	}

	for _, alterSpec := range ctx.AllAlterSpecification() {
		switch spec := alterSpec.(type) {
		case *generated.AlterByAddColumnsContext:
			colNames := spec.AllUid()
			colDefs := spec.AllColumnDefinition()
			if len(colNames) != len(colDefs) {
				return nil, fmt.Errorf("expected %d column names, got %d", len(colDefs), len(colNames))
			}

			var cols []Column
			for _, uid := range colNames {
				name, err := getTextFromSingleNodeBranch(uid)
				if err != nil {
					return nil, err
				}

				cols = append(cols, Column{Name: name})
			}

			for i, colDef := range colDefs {
				cols[i] = cols[i].buildDataTypePrimaryKey(colDef)
			}

			events = append(events, AddColumnsEvent{TableName: tableName, Columns: cols})
		case *generated.AlterByAddColumnContext:
			col, err := processAddOrModifyColumn(spec)
			if err != nil {
				return nil, err
			}

			events = append(events, AddColumnsEvent{TableName: tableName, Columns: []Column{col}})
		case *generated.AlterByModifyColumnContext:
			col, err := processAddOrModifyColumn(spec)
			if err != nil {
				return nil, err
			}

			events = append(events, ModifyColumnEvent{TableName: tableName, Column: col})
		case *generated.AlterByDropColumnContext:
			dropColEvent, err := processDropColumn(tableName, spec)
			if err != nil {
				return nil, err
			}

			events = append(events, dropColEvent)
		case *generated.AlterByAddPrimaryKeyContext:
			cols, err := processAddPrimaryKey(spec)
			if err != nil {
				return nil, err
			}

			events = append(events, AddPrimaryKeyEvent{TableName: tableName, Columns: cols})
		case *generated.AlterByRenameColumnContext:
			event, err := processRenameColumn(tableName, spec)
			if err != nil {
				return nil, err
			}

			events = append(events, event)
		default:
			slog.Warn("Unsupported alter specification", slog.String("type", fmt.Sprintf("%T", spec)))
		}
	}

	return events, nil
}

func processRenameColumn(tableName string, ctx *generated.AlterByRenameColumnContext) (RenameColumnEvent, error) {
	allUids := ctx.AllUid()
	if len(allUids) != 2 {
		// You can only do one column rename in an ALTER TABLE statement
		return RenameColumnEvent{}, fmt.Errorf("expected 2 uids, got %d", len(allUids))
	}

	oldName, err := getTextFromSingleNodeBranch(allUids[0])
	if err != nil {
		return RenameColumnEvent{}, err
	}

	newName, err := getTextFromSingleNodeBranch(allUids[1])
	if err != nil {
		return RenameColumnEvent{}, err
	}

	return RenameColumnEvent{TableName: tableName, Column: Column{Name: newName, PreviousName: oldName}}, nil
}

func processAddPrimaryKey(ctx *generated.AlterByAddPrimaryKeyContext) ([]Column, error) {
	var cols []Column
	for _, colName := range ctx.IndexColumnNames().AllIndexColumnName() {
		text, err := getTextFromSingleNodeBranch(colName)
		if err != nil {
			return nil, fmt.Errorf("failed to get text from branch: %w", err)
		}

		cols = append(cols, Column{Name: text, PrimaryKey: true})
	}

	return cols, nil
}

func processAddOrModifyColumn(ctx generated.IAlterSpecificationContext) (Column, error) {
	var names []string
	var first bool
	var after bool

	var col Column
	for _, child := range ctx.GetChildren() {
		switch castedChild := child.(type) {
		case *generated.ColumnDefinitionContext:
			col = col.buildDataTypePrimaryKey(castedChild)
		case *antlr.TerminalNodeImpl:
			text := castedChild.GetText()
			switch strings.ToUpper(text) {
			case "FIRST":
				first = true
			case "AFTER":
				after = true
			default:
				slog.Warn("Unsupported alter specification terminal node", slog.String("text", text))
			}
		case *generated.UidContext:
			name, err := getTextFromSingleNodeBranch(castedChild)
			if err != nil {
				return Column{}, fmt.Errorf("failed to get text from branch: %w", err)
			}

			names = append(names, name)
		default:
			slog.Warn("Unsupported alter specification child", slog.String("type", fmt.Sprintf("%T", castedChild)))
		}
	}

	switch len(names) {
	case 1:
		col.Name = names[0]
		if first {
			col.Position = FirstPosition{}
		}
	case 2:
		if !after {
			return Column{}, fmt.Errorf("expected after to be set if there are two names")
		}

		col.Name = names[0]
		col.Position = AfterPosition{column: names[1]}
	default:
		return Column{}, fmt.Errorf("unexpected number of names: %d", len(names))
	}

	return col, nil
}

func processDropColumn(tableName string, ctx *generated.AlterByDropColumnContext) (DropColumnsEvent, error) {
	var col Column
	for _, child := range ctx.GetChildren() {
		switch castedChild := child.(type) {
		case *generated.UidContext:
			name, err := getTextFromSingleNodeBranch(castedChild)
			if err != nil {
				return DropColumnsEvent{}, err
			}

			col.Name = name
		}
	}

	return DropColumnsEvent{TableName: tableName, Column: col}, nil
}
