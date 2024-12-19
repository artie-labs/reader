package ddl

import (
	"fmt"
	"log/slog"
	"slices"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/antlr"
)

type SchemaAdapter struct {
	adapters    map[string]TableAdapter
	tableCfgMap map[string]*config.MySQLTable
	dbName      string
	sqlMode     []string
}

func NewSchemaAdapter(cfg config.MySQL, sqlMode []string) SchemaAdapter {
	tableCfgMap := make(map[string]*config.MySQLTable)
	for _, tbl := range cfg.Tables {
		tableCfgMap[tbl.Name] = tbl
	}

	return SchemaAdapter{
		adapters:    make(map[string]TableAdapter),
		tableCfgMap: tableCfgMap,
		dbName:      cfg.Database,
		sqlMode:     sqlMode,
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
		return fmt.Errorf("failed to parse query %q: %w", query, err)
	}

	for _, result := range results {
		if err = s.applyDDL(unixTs, result); err != nil {
			return fmt.Errorf("failed to apply ddl %q: %w", query, err)
		}
	}

	return nil
}

func (s *SchemaAdapter) applyDDL(unixTs int64, result antlr.Event) error {
	switch result.(type) {
	case antlr.DropTableEvent:
		delete(s.adapters, result.GetTable())
		return nil
	case antlr.CreateTableEvent:
		var cols []Column
		for _, col := range result.GetColumns() {
			cols = append(cols, Column{
				Name:       col.Name,
				PrimaryKey: col.PrimaryKey,
				DataType:   col.DataType,
			})
		}

		tblAdapter, err := NewTableAdapter(s.dbName, s.tableCfgMap[result.GetTable()], cols, unixTs, s.sqlMode)
		if err != nil {
			return err
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

	tblAdapter, err := tblAdapter.buildGeneratedFields()
	if err != nil {
		return fmt.Errorf("failed to build generated fields: %w", err)
	}

	tblAdapter.unixTs = unixTs
	s.adapters[result.GetTable()] = tblAdapter
	return nil
}
