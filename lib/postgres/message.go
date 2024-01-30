package postgres

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/collections"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/reader/lib/postgres/debezium"
	"github.com/artie-labs/transfer/lib/size"
)

func NewMessageBuilder(table *Table, iter collections.ArrayIterator[map[string]interface{}], statsD *mtr.Client, maxRowSize uint64) collections.ArrayIterator[lib.RawMessage] {
	return collections.NewMapIterator(iter, func(row map[string]interface{}) (lib.RawMessage, bool, error) {
		start := time.Now()
		partitionKeyMap := make(map[string]interface{})
		for _, key := range table.PrimaryKeys.Keys() {
			partitionKeyMap[key] = row[key]
		}

		if maxRowSize > 0 {
			if uint64(size.GetApproxSize(row)) > maxRowSize {
				slog.Info(fmt.Sprintf("Row greater than %v mb, skipping...", maxRowSize/1024/1024), slog.Any("key", partitionKeyMap))
				return lib.RawMessage{}, true, nil
			}
		}

		payload, err := debezium.NewPayload(&debezium.NewArgs{
			TableName: table.Name,
			Columns:   table.OriginalColumns,
			Fields:    table.Config.Fields,
			RowData:   row,
		})
		if err != nil {
			return lib.RawMessage{}, false, fmt.Errorf("failed to create debezium payload: %w", err)
		}

		if statsD != nil {
			(*statsD).Timing("scanned_and_parsed", time.Since(start), map[string]string{
				"table":  strings.ReplaceAll(table.Name, `"`, ``),
				"schema": table.Schema,
			})
		}

		return lib.RawMessage{
			TopicSuffix:  table.TopicSuffix(),
			PartitionKey: partitionKeyMap,
			Payload:      payload,
		}, false, nil
	})
}
