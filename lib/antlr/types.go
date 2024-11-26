package antlr

type Column struct {
	Name string
	// Optionally set depending on the context
	PreviousName string
	DataType     string
	PrimaryKey   bool
	Position     Position
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

func (c Column) clean() Column {
	col := Column{
		Name:         unescape(c.Name),
		PreviousName: unescape(c.PreviousName),
		DataType:     c.DataType,
		PrimaryKey:   c.PrimaryKey,
		Position:     c.Position,
	}

	return col
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

type AddColumnEvent struct {
	TableName string
	Column    Column
}

func (a AddColumnEvent) GetTable() string {
	return unescape(a.TableName)
}

func (a AddColumnEvent) GetColumns() []Column {
	return []Column{a.Column.clean()}
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
