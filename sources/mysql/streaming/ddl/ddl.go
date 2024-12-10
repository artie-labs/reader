package ddl

import (
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/antlr"
	"github.com/artie-labs/reader/lib/debezium/transformer"
	"github.com/artie-labs/reader/lib/mysql/converters"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/column"
	"log/slog"
	"slices"
)

type Column struct {
	Name       string
	DataType   string
	PrimaryKey bool
}

type TableAdapter struct {
	dbName   string
	tableCfg *config.MySQLTable
	columns  []Column
	unixTs   int64
}

func (t TableAdapter) ShouldReplicate() bool {
	return t.tableCfg != nil
}

func (t TableAdapter) GetUnixTs() int64 {
	return t.unixTs
}

func NewTableAdapter(dbName string, tableCfg *config.MySQLTable, columns []Column, unixTs int64) TableAdapter {
	return TableAdapter{
		dbName:   dbName,
		tableCfg: tableCfg,
		columns:  columns,
		unixTs:   unixTs,
	}
}

func (t TableAdapter) TopicSuffix() string {
	return fmt.Sprintf("%s.%s", t.dbName, t.tableCfg.Name)
}

func (t TableAdapter) ColumnNames() []string {
	var colNames []string
	for _, col := range t.columns {
		colNames = append(colNames, col.Name)
	}

	return colNames
}

func (t TableAdapter) PartitionKeys() []string {
	var keys []string
	for _, col := range t.columns {
		if col.PrimaryKey {

			keys = append(keys, col.Name)
		}
	}

	return keys
}

func (t TableAdapter) GetParsedColumns() ([]schema.Column, error) {
	var parsedColumns []schema.Column
	for _, col := range t.columns {
		dataType, opts, err := schema.ParseColumnDataType(col.DataType)
		if err != nil {
			return nil, fmt.Errorf("failed to parse column data type: %w", err)
		}

		parsedColumns = append(parsedColumns, schema.Column{
			Name: col.Name,
			Type: dataType,
			Opts: opts,
		})
	}

	return parsedColumns, nil
}

func (t TableAdapter) GetFieldConverters() ([]transformer.FieldConverter, error) {
	//  TODO: Make this more efficient.
	parsedColumns, err := t.GetParsedColumns()
	if err != nil {
		return nil, err
	}

	// Exclude columns (if any) from the table metadata
	cols, err := column.FilterOutExcludedColumns(parsedColumns, t.tableCfg.ExcludeColumns, t.PartitionKeys())
	if err != nil {
		return nil, err
	}

	// Include columns (if any) from the table metadata
	cols, err = column.FilterForIncludedColumns(cols, t.tableCfg.IncludeColumns, t.PartitionKeys())
	if err != nil {
		return nil, err
	}

	fieldConverters := make([]transformer.FieldConverter, len(cols))
	for i, col := range cols {
		converter, err := converters.ValueConverterForType(col.Type, col.Opts)
		if err != nil {
			return nil, fmt.Errorf("failed to build value converter for column %q: %w", col.Name, err)
		}
		fieldConverters[i] = transformer.FieldConverter{Name: col.Name, ValueConverter: converter}
	}

	return fieldConverters, nil
}

type SchemaAdapter struct {
	adapters    map[string]TableAdapter
	tableCfgMap map[string]config.MySQLTable
}

func NewSchemaAdapter(cfg config.MySQL) SchemaAdapter {
	tableCfgMap := make(map[string]config.MySQLTable)
	for _, tbl := range cfg.Tables {
		tableCfgMap[tbl.Name] = *tbl
	}

	return SchemaAdapter{
		adapters:    make(map[string]TableAdapter),
		tableCfgMap: tableCfgMap,
	}
}

func (s *SchemaAdapter) GetTableAdapter(tableName string) (TableAdapter, bool) {
	tblAdapter, ok := s.adapters[tableName]
	if !ok {
		return TableAdapter{}, ok
	}

	return tblAdapter, ok
}

func (s *SchemaAdapter) ApplyDDL(unixTs int64, query string) error {
	results, err := antlr.Parse(query)
	if err != nil {
		return err
	}

	for _, result := range results {
		if err = s.applyDDL(unixTs, result); err != nil {
			return fmt.Errorf("failed to apply ddl: %w", err)
		}
	}

	return nil
}

func (s *SchemaAdapter) applyDDL(unixTs int64, result antlr.Event) error {
	if _, ok := result.(antlr.CreateTableEvent); ok {
		var cols []Column
		for _, col := range result.GetColumns() {
			cols = append(cols, Column{
				Name:       col.Name,
				PrimaryKey: col.PrimaryKey,
				DataType:   col.DataType,
			})
		}

		s.adapters[result.GetTable()] = TableAdapter{columns: cols, unixTs: unixTs}
		return nil
	}

	tblAdapter, ok := s.adapters[result.GetTable()]
	if !ok {
		return fmt.Errorf("table not found: %q", result.GetTable())
	}

	switch castedResult := result.(type) {
	case antlr.RenameColumnEvent:
		for _, col := range castedResult.GetColumns() {
			columnIdx := slices.IndexFunc(tblAdapter.columns, func(x Column) bool { return x.Name == col.PreviousName })
			if columnIdx == -1 {
				return fmt.Errorf("column not found: %q", col.PreviousName)
			}

			// Apply the new name
			tblAdapter.columns[columnIdx].Name = col.Name
		}
	case antlr.AddPrimaryKeyEvent:
		for _, col := range castedResult.GetColumns() {
			columnIdx := slices.IndexFunc(tblAdapter.columns, func(x Column) bool { return x.Name == col.Name })
			if columnIdx == -1 {
				return fmt.Errorf("column not found: %q", col.Name)
			}

			tblAdapter.columns[columnIdx].PrimaryKey = true
		}
	case antlr.DropColumnsEvent:
		for _, col := range castedResult.GetColumns() {
			columnIdx := slices.IndexFunc(tblAdapter.columns, func(x Column) bool { return x.Name == col.Name })
			if columnIdx == -1 {
				return fmt.Errorf("column not found: %q", col.Name)
			}

			tblAdapter.columns = slices.Delete(tblAdapter.columns, columnIdx, columnIdx+1)
		}

		s.adapters[castedResult.GetTable()] = tblAdapter
	case antlr.ModifyColumnEvent:
		for _, col := range castedResult.GetColumns() {
			columnIdx := slices.IndexFunc(tblAdapter.columns, func(x Column) bool { return x.Name == col.Name })
			if columnIdx == -1 {
				return fmt.Errorf("column not found: %q", col.Name)
			}

			tblAdapter.columns[columnIdx].DataType = col.DataType
		}
	case antlr.AddColumnsEvent:
		for _, col := range castedResult.GetColumns() {
			if slices.ContainsFunc(tblAdapter.columns, func(x Column) bool { return x.Name == col.Name }) {
				return fmt.Errorf("column already exists: %q", col.Name)
			}

			tblAdapter.columns = append(tblAdapter.columns, Column{
				Name:     col.Name,
				DataType: col.DataType,
			})
		}
	default:
		slog.Info("Skipping event type", slog.Any("eventType", fmt.Sprintf("%T", result)))
	}

	for _, col := range result.GetColumns() {
		if col.Position != nil {
			switch castedPosition := col.Position.(type) {
			case antlr.FirstPosition:
				// Find the current position, delete it and insert it at the first position
				columnIdx := slices.IndexFunc(tblAdapter.columns, func(x Column) bool { return x.Name == col.Name })
				if columnIdx == -1 {
					return fmt.Errorf("column not found: %q", col.Name)
				}

				_col := tblAdapter.columns[columnIdx]
				// Delete the column
				tblAdapter.columns = slices.Delete(tblAdapter.columns, columnIdx, columnIdx+1)
				// Then insert it at the first position
				tblAdapter.columns = slices.Insert(tblAdapter.columns, 0, _col)
			case antlr.AfterPosition:
				// Find the current position, delete it and insert it after the specified column
				columnIdx := slices.IndexFunc(tblAdapter.columns, func(x Column) bool { return x.Name == col.Name })
				if columnIdx == -1 {
					return fmt.Errorf("column not found: %q", col.Name)
				}

				_col := tblAdapter.columns[columnIdx]
				// Delete the column
				tblAdapter.columns = slices.Delete(tblAdapter.columns, columnIdx, columnIdx+1)

				// Find the column to insert after
				afterColumnIdx := slices.IndexFunc(tblAdapter.columns, func(x Column) bool { return x.Name == castedPosition.Column() })
				if afterColumnIdx == -1 {
					return fmt.Errorf("column not found: %q", castedPosition.Column())
				}

				// Insert the column after the specified column
				tblAdapter.columns = slices.Insert(tblAdapter.columns, afterColumnIdx+1, _col)
			default:
				return fmt.Errorf("unknown position type: %T", castedPosition)
			}
		}
	}

	tblAdapter.unixTs = unixTs
	s.adapters[result.GetTable()] = tblAdapter
	return nil
}
