package antlr

import (
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/artie-labs/reader/lib/antlr/generated"
)

func (c Column) buildDataTypePrimaryKey(ctx generated.IColumnDefinitionContext) (Column, error) {
	returnedCol := Column{
		Name: c.Name,
	}

	for _, constraint := range ctx.AllColumnConstraint() {
		switch castedConstraint := constraint.(type) {
		case *generated.PrimaryKeyColumnConstraintContext:
			returnedCol.PrimaryKey = true
		case *generated.DefaultColumnConstraintContext:
			returnedCol.DefaultValue = parseDefaultValue(castedConstraint.DefaultValue())
		}
	}

	var parts []string
	for _, child := range ctx.DataType().GetChildren() {
		switch castedChild := child.(type) {
		case *generated.CharSetContext:
			for _, node := range castedChild.GetChildren() {
				part, err := getTextFromSingleNodeBranch(node)
				if err != nil {
					return Column{}, fmt.Errorf("failed to extract charset: %w", err)
				}

				parts = append(parts, part)
			}

		default:
			if pt, ok := child.(antlr.ParseTree); ok {
				parts = append(parts, pt.GetText())
			} else {
				slog.Warn("Skipping type that is not a parse tree", slog.String("type", fmt.Sprintf("%T", child)))
			}
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

	returnedCol.DataType = dataType
	return returnedCol, nil
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
			var err error
			col, err = col.buildDataTypePrimaryKey(t)
			if err != nil {
				return Column{}, fmt.Errorf("failed to build data type primary key: %w", err)
			}
		}
	}

	return col, nil
}

func processPrimaryKeyConstraintNode(node *generated.PrimaryKeyTableConstraintContext) ([]string, error) {
	var colNames []string
	for _, child := range node.IndexColumnNames().GetChildren() {
		if casted, ok := child.(*generated.IndexColumnNameContext); ok {
			colName, err := getTextFromSingleNodeBranch(casted)
			if err != nil {
				return nil, err
			}

			colNames = append(colNames, colName)
		}
	}

	return colNames, nil
}

func processCopyTable(ctx *generated.CopyCreateTableContext) (Event, error) {
	tableNames := ctx.AllTableName()
	if len(tableNames) != 2 {
		return nil, fmt.Errorf("expected exactly 2 table names, got %d", len(tableNames))
	}

	tableName, err := getTextFromSingleNodeBranch(tableNames[0])
	if err != nil {
		return nil, err
	}

	copiedFromTableName, err := getTextFromSingleNodeBranch(tableNames[1])
	if err != nil {
		return nil, err
	}

	return CopyTableEvent{TableName: tableName, CopyFromTableName: copiedFromTableName}, nil
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
			for _, node := range castedChild.GetChildren() {
				switch castedNode := node.(type) {
				case *generated.ColumnDeclarationContext:
					_col, err := processColumn(castedNode)
					if err != nil {
						return nil, err
					}

					columns = append(columns, _col)
				case *generated.ConstraintDeclarationContext:
					for _, constraintChild := range castedNode.GetChildren() {
						if casted, ok := constraintChild.(*generated.PrimaryKeyTableConstraintContext); ok {
							colNames, err := processPrimaryKeyConstraintNode(casted)
							if err != nil {
								return nil, err
							}

							for _, colName := range colNames {
								columnIdx := slices.IndexFunc(columns, func(x Column) bool { return x.Name == colName })
								if columnIdx == -1 {
									return nil, fmt.Errorf("failed to find column %q", colName)
								}

								columns[columnIdx].PrimaryKey = true
							}
						}
					}
				}
			}
		default:
			slog.Warn(fmt.Sprintf("Skipping unsupported create table types: %T", castedChild))
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
