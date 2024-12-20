package antlr

import "github.com/artie-labs/transfer/lib/typing"

type Column struct {
	Name string
	// Optionally set depending on the context
	PreviousName string
	DataType     string
	DefaultValue *string
	PrimaryKey   bool
	Position     Position
}

func (c Column) clean() Column {
	col := Column{
		Name:         unescape(c.Name),
		PreviousName: unescape(c.PreviousName),
		DataType:     c.DataType,
		PrimaryKey:   c.PrimaryKey,
		Position:     c.Position,
	}

	if c.DefaultValue != nil {
		col.DefaultValue = typing.ToPtr(baseUnescape(*c.DefaultValue, `'`))
	}

	return col
}

type Event interface {
	GetTable() string
	GetColumns() []Column
}

type Position interface {
	Kind() string
}

type FirstPosition struct{}

func (f FirstPosition) Kind() string {
	return "first"
}

type AfterPosition struct {
	column string
}

func (a AfterPosition) Column() string {
	return unescape(a.column)
}

func (a AfterPosition) Kind() string {
	return "after"
}

type RenameTableEvent struct {
	tableName    string
	newTableName string
}

func (r RenameTableEvent) GetTable() string {
	return unescape(r.tableName)
}

func (r RenameTableEvent) GetNewTableName() string {
	return unescape(r.newTableName)
}

func (r RenameTableEvent) GetColumns() []Column {
	return nil
}

type CopyTableEvent struct {
	tableName         string
	copyFromTableName string
}

func (c CopyTableEvent) GetTable() string {
	return unescape(c.tableName)
}

func (c CopyTableEvent) GetCopyFromTableName() string {
	return unescape(c.copyFromTableName)
}

func (c CopyTableEvent) GetColumns() []Column {
	return nil
}

type CreateTableEvent struct {
	TableName string
	Columns   []Column
}

func (c CreateTableEvent) GetTable() string {
	return unescape(c.TableName)
}

func (c CreateTableEvent) GetColumns() []Column {
	var cols []Column
	for _, col := range c.Columns {
		cols = append(cols, col.clean())
	}

	return cols
}

type RenameColumnEvent struct {
	TableName string
	Column    Column
}

func (r RenameColumnEvent) GetTable() string {
	return unescape(r.TableName)
}

func (r RenameColumnEvent) GetColumns() []Column {
	return []Column{r.Column.clean()}
}

type AddPrimaryKeyEvent struct {
	TableName string
	Columns   []Column
}

func (a AddPrimaryKeyEvent) GetTable() string {
	return unescape(a.TableName)
}

func (a AddPrimaryKeyEvent) GetColumns() []Column {
	var cols []Column
	for _, col := range a.Columns {
		cols = append(cols, col.clean())
	}

	return cols
}

type ModifyColumnEvent struct {
	TableName string
	Column    Column
}

func (a ModifyColumnEvent) GetTable() string {
	return unescape(a.TableName)
}

func (a ModifyColumnEvent) GetColumns() []Column {
	return []Column{a.Column.clean()}
}

type DropColumnsEvent struct {
	TableName string
	Column    Column
}

func (d DropColumnsEvent) GetTable() string {
	return unescape(d.TableName)
}

func (d DropColumnsEvent) GetColumns() []Column {
	return []Column{d.Column.clean()}
}

type AddColumnsEvent struct {
	TableName string
	Columns   []Column
}

func (a AddColumnsEvent) GetTable() string {
	return unescape(a.TableName)
}

func (a AddColumnsEvent) GetColumns() []Column {
	var cols []Column
	for _, col := range a.Columns {
		cols = append(cols, col.clean())
	}

	return cols
}

type DropTableEvent struct {
	TableName string
}

func (d DropTableEvent) GetTable() string {
	return unescape(d.TableName)
}

func (d DropTableEvent) GetColumns() []Column {
	return nil
}
