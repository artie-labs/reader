package streaming

import (
	"fmt"
	"github.com/artie-labs/reader/lib/antlr"
	"github.com/go-mysql-org/go-mysql/replication"
	"log/slog"
	"slices"
	"time"
)

type Column struct {
	Name     string
	DataType string
}

type TableAdapter struct {
	columns     []Column
	primaryKeys []string
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

	fmt.Println("Processing DDL", query)

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
				Name:     col.Name,
				DataType: col.DataType,
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
			tblAdapter.primaryKeys = append(tblAdapter.primaryKeys, col.Name)
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

			tblAdapter.columns = append(tblAdapter.columns[:columnIdx], tblAdapter.columns[columnIdx+1:]...)
		}
	case antlr.AddColumnsEvent:
		tblAdapter, ok := s.adapters[castedResult.GetTable()]
		if !ok {
			return fmt.Errorf("table not found: %q", castedResult.GetTable())
		}

		for _, col := range castedResult.GetColumns() {
			// TODO: Handle position
			tblAdapter.columns = append(tblAdapter.columns, Column{
				Name:     col.Name,
				DataType: col.DataType,
			})
		}
	default:
		slog.Info("Skipping event type", slog.Any("eventType", fmt.Sprintf("%T", result)))
	}

	return nil
}
