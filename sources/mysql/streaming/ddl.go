package streaming

import (
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/lib/antlr"
)

type Column struct {
	Name       string
	DataType   string
	PrimaryKey bool
}

type TableAdapter struct {
	columns []Column
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
		Query: query,
		Ts:    ts,
	}

	if err := i.schemaHistoryList.Push(schemaHistory); err != nil {
		return fmt.Errorf("failed to push schema history: %w", err)
	}

	return i.schemaAdapter.ApplyDDL(query)
}

func (s *SchemaAdapter) ApplyDDL(query string) error {
	results, err := antlr.Parse(query)
	if err != nil {
		return err
	}

	for _, result := range results {
		if err = s.applyDDL(result); err != nil {
			return fmt.Errorf("failed to apply ddl: %w", err)
		}
	}

	return nil
}

func (s *SchemaAdapter) applyDDL(result antlr.Event) error {
	switch castedResult := result.(type) {
	case antlr.CreateTableEvent:
		var cols []Column
		for _, col := range castedResult.GetColumns() {
			cols = append(cols, Column{
				Name:       col.Name,
				PrimaryKey: col.PrimaryKey,
				DataType:   col.DataType,
			})
		}

		s.adapters[castedResult.TableName] = TableAdapter{columns: cols}
	case antlr.RenameColumnEvent:
		tblAdapter, ok := s.adapters[castedResult.GetTable()]
		if !ok {
			return fmt.Errorf("table not found: %q", castedResult.GetTable())
		}

		for _, col := range castedResult.GetColumns() {
			columnIdx := slices.IndexFunc(tblAdapter.columns, func(x Column) bool { return x.Name == col.PreviousName })
			if columnIdx == -1 {
				return fmt.Errorf("column not found: %q", col.PreviousName)
			}

			// Apply the new name
			tblAdapter.columns[columnIdx].Name = col.Name
		}
	case antlr.AddPrimaryKeyEvent:
		tblAdapter, ok := s.adapters[castedResult.GetTable()]
		if !ok {
			return fmt.Errorf("table not found: %q", castedResult.GetTable())
		}

		for _, col := range castedResult.GetColumns() {
			columnIdx := slices.IndexFunc(tblAdapter.columns, func(x Column) bool { return x.Name == col.Name })
			if columnIdx == -1 {
				return fmt.Errorf("column not found: %q", col.Name)
			}

			// Update pk
			tblAdapter.columns[columnIdx].PrimaryKey = true
		}
	case antlr.ModifyColumnEvent:
		tblAdapter, ok := s.adapters[castedResult.GetTable()]
		if !ok {
			return fmt.Errorf("table not found: %q", castedResult.GetTable())
		}

		for _, col := range castedResult.GetColumns() {
			columnIdx := slices.IndexFunc(tblAdapter.columns, func(x Column) bool { return x.Name == col.Name })
			if columnIdx == -1 {
				return fmt.Errorf("column not found: %q", col.Name)
			}

			// TODO: Handle position
			tblAdapter.columns[columnIdx].DataType = col.DataType
		}
	case antlr.DropColumnsEvent:
		tblAdapter, ok := s.adapters[castedResult.GetTable()]
		if !ok {
			return fmt.Errorf("table not found: %q", castedResult.GetTable())
		}

		for _, col := range castedResult.GetColumns() {
			columnIdx := slices.IndexFunc(tblAdapter.columns, func(x Column) bool { return x.Name == col.Name })
			if columnIdx == -1 {
				return fmt.Errorf("column not found: %q", col.Name)
			}

			tblAdapter.columns = slices.Delete(tblAdapter.columns, columnIdx, columnIdx+1)
		}

		s.adapters[castedResult.GetTable()] = tblAdapter
	case antlr.AddColumnsEvent:
		tblAdapter, ok := s.adapters[castedResult.GetTable()]
		if !ok {
			return fmt.Errorf("table not found: %q", castedResult.GetTable())
		}

		for _, col := range castedResult.GetColumns() {
			// Make sure column does not already exist
			if slices.ContainsFunc(tblAdapter.columns, func(x Column) bool { return x.Name == col.Name }) {
				return fmt.Errorf("column already exists: %q", col.Name)
			}

			// TODO: Handle position
			tblAdapter.columns = append(tblAdapter.columns, Column{
				Name:     col.Name,
				DataType: col.DataType,
			})
		}

		s.adapters[castedResult.GetTable()] = tblAdapter
	default:
		slog.Info("Skipping event type", slog.Any("eventType", fmt.Sprintf("%T", result)))
	}

	return nil
}
