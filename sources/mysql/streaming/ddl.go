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

type TableAdapter struct {
	unixTs  int64
	columns []Column

	// Generated
	fieldConverters []transformer.FieldConverter

	// These are injected when we retrieve tableAdapter.
	tableCfg config.MySQLTable
	dbName   string
}

func NewTableAdapter(cols []Column, unixTs int64) (TableAdapter, error) {
	tblAdapter := TableAdapter{columns: cols, unixTs: unixTs}
	if err := tblAdapter.buildFieldConverters(); err != nil {
		return TableAdapter{}, fmt.Errorf("failed to build field converters: %w", err)
	}

	return tblAdapter, nil
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

func (t TableAdapter) GetFieldConverters() []transformer.FieldConverter {
	return t.fieldConverters
}

func (t TableAdapter) buildFieldConverters() error {
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

	// Exclude columns (if any) from the table metadata
	cols, err := column.FilterOutExcludedColumns(parsedColumns, t.tableCfg.ExcludeColumns, t.PartitionKeys())
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

type SchemaAdapter struct {
	adapters map[string]TableAdapter
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

	return i.schemaAdapter.ApplyDDL(query, ts.Unix())
}

func (s *SchemaAdapter) ApplyDDL(query string, unixTs int64) error {
	results, err := antlr.Parse(query)
	if err != nil {
		return err
	}

	for _, result := range results {
		if err = s.applyDDL(result, unixTs); err != nil {
			return fmt.Errorf("failed to apply ddl: %w", err)
		}
	}

	return nil
}

func (s *SchemaAdapter) applyDDL(result antlr.Event, unixTs int64) error {
	if _, ok := result.(antlr.CreateTableEvent); ok {
		var cols []Column
		for _, col := range result.GetColumns() {
			cols = append(cols, Column{
				Name:       col.Name,
				PrimaryKey: col.PrimaryKey,
				DataType:   col.DataType,
			})
		}

		tblAdapter, err := NewTableAdapter(cols, unixTs)
		if err != nil {
			return fmt.Errorf("failed to create table adapter: %w", err)
		}

		s.adapters[result.GetTable()] = tblAdapter
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
	case antlr.ModifyColumnEvent:
		for _, col := range castedResult.GetColumns() {
			columnIdx := slices.IndexFunc(tblAdapter.columns, func(x Column) bool { return x.Name == col.Name })
			if columnIdx == -1 {
				return fmt.Errorf("column not found: %q", col.Name)
			}

			// TODO: Handle position
			tblAdapter.columns[columnIdx].DataType = col.DataType
		}
	case antlr.DropColumnsEvent:
		for _, col := range castedResult.GetColumns() {
			columnIdx := slices.IndexFunc(tblAdapter.columns, func(x Column) bool { return x.Name == col.Name })
			if columnIdx == -1 {
				return fmt.Errorf("column not found: %q", col.Name)
			}

			tblAdapter.columns = slices.Delete(tblAdapter.columns, columnIdx, columnIdx+1)
		}
	case antlr.AddColumnsEvent:
		for _, col := range castedResult.GetColumns() {
			if slices.ContainsFunc(tblAdapter.columns, func(x Column) bool { return x.Name == col.Name }) {
				return fmt.Errorf("column already exists: %q", col.Name)
			}

			// TODO: Handle position
			tblAdapter.columns = append(tblAdapter.columns, Column{
				Name:     col.Name,
				DataType: col.DataType,
			})
		}
	default:
		slog.Info("Skipping event type", slog.Any("eventType", fmt.Sprintf("%T", result)))
	}

	tblAdapter.unixTs = unixTs
	if err := tblAdapter.buildFieldConverters(); err != nil {
		return fmt.Errorf("failed to build field converters: %w", err)
	}

	s.adapters[result.GetTable()] = tblAdapter
	return nil
}
