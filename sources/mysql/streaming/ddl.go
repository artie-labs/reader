package streaming

import (
	"fmt"
	"github.com/artie-labs/reader/lib/antlr"
	"github.com/go-mysql-org/go-mysql/replication"
	"log/slog"
	"time"
)

type TableAdapter struct {
	columns     []string
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

	schemaHistory := SchemaHistory{
		Query: string(evt.Query),
		Ts:    ts,
	}

	if err := i.schemaHistoryList.Push(schemaHistory); err != nil {
		return fmt.Errorf("failed to push schema history: %w", err)
	}

	// Then process the DDL
	return nil
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
	switch result.(type) {
	case antlr.CreateTableEvent:
		// TODO
	case antlr.RenameColumnEvent:
		// TODO
	case antlr.AddPrimaryKeyEvent:
		// TODO
	case antlr.ModifyColumnEvent:
		// TODO
	case antlr.DropColumnsEvent:
		//	TODO
	case antlr.AddColumnsEvent:
		// TODO
	default:
		slog.Info("Skipping event type", slog.Any("eventType", fmt.Sprintf("%T", result)))
		return nil
	}
}
