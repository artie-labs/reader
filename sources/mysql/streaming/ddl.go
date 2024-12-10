package streaming

import (
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/antlr"
	"github.com/artie-labs/reader/lib/debezium/transformer"
	"github.com/artie-labs/reader/lib/mysql/converters"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/column"
)

type Column struct {
	Name       string
	DataType   string
	PrimaryKey bool
}

func NewTableAdapter(columns []Column, unixTs int64, cfg config.MySQLTable, dbName string) TableAdapter {
	return TableAdapter{columns: columns, unixTs: unixTs, dirty: true, tableCfg: cfg, dbName: dbName}
}

type TableAdapter struct {
	columns []Column
	unixTs  int64
	dirty   bool

	// These are injected when we retrieve tableAdapter.
	tableCfg config.MySQLTable
	dbName   string

	// Generated

	parsedColumns   []schema.Column
	fieldConverters []transformer.FieldConverter
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

func (t *TableAdapter) buildFieldConverters() error {
	// Exclude columns (if any) from the table metadata
	cols, err := column.FilterOutExcludedColumns(t.GetParsedColumns(), t.tableCfg.ExcludeColumns, t.PartitionKeys())
	if err != nil {
		return err
	}

	// Include columns (if any) from the table metadata
	cols, err = column.FilterForIncludedColumns(cols, t.tableCfg.IncludeColumns, t.PartitionKeys())
	if err != nil {
		return err
	}

	fieldConverters := make([]transformer.FieldConverter, len(cols))
	for i, col := range cols {
		converter, err := converters.ValueConverterForType(col.Type, col.Opts)
		if err != nil {
			return fmt.Errorf("failed to build value converter for column %q: %w", col.Name, err)
		}
		fieldConverters[i] = transformer.FieldConverter{Name: col.Name, ValueConverter: converter}
	}

	t.fieldConverters = fieldConverters
	return nil
}

func (t *TableAdapter) buildParsedColumns() error {
	var parsedColumns []schema.Column
	for _, col := range t.columns {
		dataType, opts, err := schema.ParseColumnDataType(col.DataType)
		if err != nil {
			return fmt.Errorf("failed to parse column data type: %w", err)
		}

		parsedColumns = append(parsedColumns, schema.Column{
			Name: col.Name,
			Type: dataType,
			Opts: opts,
		})
	}

	t.parsedColumns = parsedColumns
	return nil
}

func (t *TableAdapter) Rebuild() error {
	if !t.dirty {
		return nil
	}

	if err := t.buildParsedColumns(); err != nil {
		return fmt.Errorf("failed to build parsed columns: %w", err)
	}

	if err := t.buildFieldConverters(); err != nil {
		return fmt.Errorf("failed to build field converters: %w", err)
	}

	t.dirty = false
	return nil
}

func (t TableAdapter) GetParsedColumns() []schema.Column {
	return t.parsedColumns
}

func (t TableAdapter) GetFieldConverters() []transformer.FieldConverter {
	return t.fieldConverters
}

type SchemaAdapter struct {
	adapters    map[string]TableAdapter
	dbName      string
	tableCfgMap map[string]config.MySQLTable
}

func NewSchemaAdapter(cfg config.MySQL) SchemaAdapter {
	tableCfgMap := make(map[string]config.MySQLTable)
	for _, tbl := range cfg.Tables {
		tableCfgMap[tbl.Name] = *tbl
	}

	return SchemaAdapter{
		adapters:    make(map[string]TableAdapter),
		dbName:      cfg.Database,
		tableCfgMap: tableCfgMap,
	}
}

func (i *Iterator) persistAndProcessDDL(evt *replication.QueryEvent, ts time.Time) error {
	if evt.ErrorCode != 0 {
		// Don't process a non-zero error code DDL.
		return nil
	}

	query := string(evt.Query)
	schemaHistory := SchemaHistory{
		Query:  query,
		UnixTs: ts.Unix(),
	}

	if err := i.schemaHistoryList.Push(schemaHistory); err != nil {
		return fmt.Errorf("failed to push schema history: %w", err)
	}

	return i.schemaAdapter.ApplyDDL(ts.Unix(), query)
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

		var tableCfg config.MySQLTable
		if cfg, ok := s.tableCfgMap[result.GetTable()]; ok {
			tableCfg = cfg
		}

		s.adapters[result.GetTable()] = NewTableAdapter(cols, unixTs, tableCfg, s.dbName)
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
	tblAdapter.dirty = true
	s.adapters[result.GetTable()] = tblAdapter
	return nil
}
