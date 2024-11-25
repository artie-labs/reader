package antlr

import (
	"fmt"
	"github.com/antlr4-go/antlr/v4"
	"github.com/artie-labs/reader/lib/antlr/parser"
	"strings"
)

var MySQLUnescapeFunction = func(s string) string {
	if strings.HasPrefix(s, "`") && strings.HasSuffix(s, "`") {
		return s[1 : len(s)-1]
	}

	return s
}

type UnescapeFunction func(string) string

type Event interface {
	GetTables(unescape UnescapeFunction) []string
	GetColumns(unescape UnescapeFunction) []Column
}

type DropTableEvent struct {
	TableNames []string
}

func (d DropTableEvent) GetTables(unescape UnescapeFunction) []string {
	var tables []string
	for _, table := range d.TableNames {
		tables = append(tables, unescape(table))
	}

	return tables
}

func (d DropTableEvent) GetColumns(_ UnescapeFunction) []Column {
	return nil
}

type Column struct {
	Name     string
	DataType string
}

type CreateTableEvent struct {
	TableName string
	Columns   []Column
}

func (c CreateTableEvent) GetTables(unescape UnescapeFunction) []string {
	return []string{unescape(c.TableName)}
}

func (c CreateTableEvent) GetColumns(unescape UnescapeFunction) []Column {
	var cols []Column
	for _, col := range c.Columns {
		cols = append(cols, Column{
			Name:     unescape(col.Name),
			DataType: col.DataType,
		})
	}

	return cols
}

type DropColumnsEvent struct {
	TableName string
	Columns   []Column
}

func (d DropColumnsEvent) GetTables(unescape UnescapeFunction) []string {
	return []string{unescape(d.TableName)}
}

func (d DropColumnsEvent) GetColumns(unescape UnescapeFunction) []Column {
	var cols []Column
	for _, col := range d.Columns {
		cols = append(cols, Column{Name: unescape(col.Name)})
	}

	return cols
}

type AddColumnsEvent struct {
	TableName string
	Columns   []Column
}

func (a AddColumnsEvent) GetTables(unescape UnescapeFunction) []string {
	return []string{unescape(a.TableName)}
}

func (a AddColumnsEvent) GetColumns(unescape UnescapeFunction) []Column {
	var cols []Column
	for _, col := range a.Columns {
		cols = append(cols, Column{
			Name:     unescape(col.Name),
			DataType: col.DataType,
		})
	}

	return cols
}

func Parse(sqlCmd string) ([]Event, error) {
	lexer := parser.NewMySqlLexer(antlr.NewInputStream(sqlCmd))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// Create a parser instance
	sqlParser := parser.NewMySqlParser(stream)
	tree := sqlParser.Root() // Replace `Root` with your grammar's root rule

	fmt.Println(tree.ToStringTree(nil, sqlParser))
	visitor := &SQLVisitor{}
	return visitor.Visit(tree)
}

type SQLVisitor struct {
	parser.BaseMySqlParserVisitor
}

func (v *SQLVisitor) Visit(tree antlr.Tree) ([]Event, error) {
	switch ctx := tree.(type) {
	case *parser.RootContext:
		for _, child := range ctx.GetChildren() {
			fmt.Println(fmt.Sprintf("child: %T", child))
		}
		// Extract the first child of the root context and process it
		if ctx.GetChildCount() == 2 {
			return v.Visit(ctx.GetChild(0))
		}

		return nil, fmt.Errorf("expected root context to have exactly one child, got: %d", ctx.GetChildCount())
	case *parser.SqlStatementsContext:
		var events []Event
		for _, child := range ctx.GetChildren() {
			evt, err := v.Visit(child)
			if err != nil {
				return nil, err
			}

			events = append(events, evt...)
		}

		return events, nil
	case *parser.SqlStatementContext:
		return v.Visit(ctx.GetChild(0))
	case *parser.DdlStatementContext:
		return v.Visit(ctx.GetChild(0))
	case *parser.CreateTableContext:
		evt, err := v.visitCreateTable(ctx)
		if err != nil {
			return nil, err
		}

		return []Event{evt}, nil
	case *parser.AlterTableContext:
		evt, err := v.visitAlterTable(ctx)
		if err != nil {
			return nil, err
		}

		return []Event{evt}, nil
	case *parser.DropTableContext:
		evt, err := v.visitDropTable(ctx)
		if err != nil {
			return nil, err
		}

		return []Event{evt}, nil
	case *parser.EmptyStatement_Context:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported context type: %T", ctx)
	}
}

func (v *SQLVisitor) visitDropTable(ctx *parser.DropTableContext) (Event, error) {
	var tableNames []string

	// Extract table names from the DropTableContext
	for _, child := range ctx.GetChildren() {
		if tableCtx, ok := child.(*parser.TablesContext); ok {
			for _, tblName := range tableCtx.AllTableName() {
				tableNames = append(tableNames, tblName.GetText())
			}
		}
	}

	if len(tableNames) == 0 {
		return nil, fmt.Errorf("failed to extract table names")
	}

	// Return a DropTablesEvent with the list of table names
	return DropTableEvent{TableNames: tableNames}, nil
}

func (v *SQLVisitor) visitCreateTable(ctx *parser.CreateTableContext) (Event, error) {
	// Extract table name
	var tableName string
	for _, child := range ctx.GetChildren() {
		if tableCtx, ok := child.(*parser.TableNameContext); ok {
			tableName = tableCtx.GetText()
			break
		}
	}

	if tableName == "" {
		return nil, fmt.Errorf("failed to extract table name")
	}

	// Extract column definitions
	var columns []Column
	for _, child := range ctx.GetChildren() {
		if colDefCtx, ok := child.(*parser.ColumnDefinitionContext); ok {
			var columnName, dataType string

			// Traverse children of ColumnDefinitionContext
			for _, colChild := range colDefCtx.GetChildren() {
				switch t := colChild.(type) {
				case *parser.UidContext:
					columnName = t.GetText()
				case *parser.DataTypeContext:
					dataType = t.GetText()
				}
			}

			// Append the column if valid
			if columnName != "" && dataType != "" {
				columns = append(columns, Column{Name: columnName, DataType: dataType})
			}
		}
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("failed to extract columns")
	}

	// Return the CreateTableEvent
	return CreateTableEvent{
		TableName: tableName,
		Columns:   columns,
	}, nil
}
func (v *SQLVisitor) visitAlterTable(ctx *parser.AlterTableContext) (Event, error) {
	tableName := ctx.TableName().GetText()
	for _, alterSpec := range ctx.AllAlterSpecification() {
		switch spec := alterSpec.(type) {
		case *parser.AlterByAddColumnContext:
			return v.visitAddColumn(tableName, spec)
		case *parser.AlterByDropColumnContext:
			return v.visitDropColumn(tableName, spec)
		default:
			return nil, fmt.Errorf("unsupported alter table operation, type: %T", spec)
		}
	}

	return nil, fmt.Errorf("no alter table operation found")
}

func (v *SQLVisitor) visitAddColumn(tableName string, ctx *parser.AlterByAddColumnContext) (Event, error) {
	var columns []Column
	for _, child := range ctx.GetChildren() {
		colDefCtx, ok := child.(*parser.ColumnDefinitionContext)
		if !ok {
			return nil, fmt.Errorf("expected type *parser.ColumnDefinitionContext, got: %T", child)
		}

		// Check if the child is a ColumnDefinitionContext
		// Extract the column name and data type
		var columnName, dataType string

		// Iterate through children of ColumnDefinitionContext to find the UID and DataType
		for _, colChild := range colDefCtx.GetChildren() {
			switch t := colChild.(type) {
			case *parser.UidContext:
				columnName = t.GetText() // UID typically represents the column name
			case *parser.DataTypeContext:
				dataType = t.GetText() // DataType represents the data type
			}
		}

		// Append the column information if both name and type were found
		if columnName != "" && dataType != "" {
			columns = append(columns, Column{Name: columnName, DataType: dataType})
		}
	}

	return AddColumnsEvent{TableName: tableName, Columns: columns}, nil
}

func (v *SQLVisitor) visitDropColumn(tableName string, ctx *parser.AlterByDropColumnContext) (Event, error) {
	var columns []Column
	for _, child := range ctx.GetChildren() {
		columnNameCtx, ok := child.(*parser.UidContext)
		if !ok {
			return nil, fmt.Errorf("expected type *parser.UidContext, got: %T", child)
		}

		columns = append(columns, Column{Name: columnNameCtx.GetText()})
	}

	return DropColumnsEvent{TableName: tableName, Columns: columns}, nil
}
