package antlr

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/artie-labs/reader/lib/antlr/generated"
)

func (c Column) buildDataTypePrimaryKey(ctx generated.IColumnDefinitionContext) Column {
	var pk bool
	for _, defChild := range ctx.AllColumnConstraint() {
		switch defChild.(type) {
		case *generated.PrimaryKeyColumnConstraintContext:
			pk = true
		}
	}

	var parts []string
	for _, child := range ctx.DataType().GetChildren() {
		if pt, ok := child.(antlr.ParseTree); ok {
			parts = append(parts, pt.GetText())
		} else {
			slog.Warn("Skipping type that is not a parse tree", slog.String("type", fmt.Sprintf("%T", child)))
		}
	}

	var dataType string
	for idx, part := range parts {
		switch idx {
		case 0:
			dataType = part
		default:
			if strings.HasPrefix(part, "(") {
				dataType += part
			} else {
				dataType += " " + part
			}
		}
	}

	return Column{
		Name:       c.Name,
		DataType:   dataType,
		PrimaryKey: pk,
	}
}

func processColumn(ctx *generated.ColumnDeclarationContext) (Column, error) {
	var col Column
	for _, colChild := range ctx.GetChildren() {
		switch t := colChild.(type) {
		case *generated.FullColumnNameContext:
			colName, err := getTextFromSingleNodeBranch(t)
			if err != nil {
				return Column{}, err
			}

			col.Name = colName
		case *generated.ColumnDefinitionContext:
			col = col.buildDataTypePrimaryKey(t)
		}
	}

	return col, nil
}

func processCreateTable(ctx *generated.ColumnCreateTableContext) (Event, error) {
	tableName, err := getTableNameFromNode(ctx.TableName())
	if err != nil {
		return nil, err
	}

	var columns []Column
	for _, child := range ctx.GetChildren() {
		switch castedChild := child.(type) {
		case *generated.CreateDefinitionsContext:
			for _, _child := range castedChild.GetChildren() {
				if colContext, ok := _child.(*generated.ColumnDeclarationContext); ok {
					_col, err := processColumn(colContext)
					if err != nil {
						return nil, err
					}

					columns = append(columns, _col)
				}
			}
		}
	}

	if tableName == "" {
		return nil, fmt.Errorf("failed to extract table name")
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("failed to extract columns")
	}

	return CreateTableEvent{TableName: tableName, Columns: columns}, nil
}
