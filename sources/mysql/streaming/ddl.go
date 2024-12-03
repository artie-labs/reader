package streaming

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	"time"
)

func (i *Iterator) persistAndProcessDDL(evt *replication.QueryEvent, ts time.Time) error {
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
