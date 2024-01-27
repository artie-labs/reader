package postgres

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/reader/lib/postgres/debezium"
	"github.com/artie-labs/transfer/lib/size"
)

const defaultErrorRetries = 10

type TableIterator struct {
	statsD        *mtr.Client
	maxRowSize    uint64
	postgresTable *Table
	scanner       scanner
}

func LoadTable(db *sql.DB, table *config.PostgreSQLTable, statsD *mtr.Client, maxRowSize uint64) (*TableIterator, error) {
	slog.Info("Loading configuration for table", slog.String("table", table.Name))

	postgresTable := NewTable(table)
	if err := postgresTable.RetrieveColumns(db); err != nil {
		if NoRowsError(err) {
			slog.Info("Table does not contain any rows, skipping...", slog.String("table", table.Name))
			return nil, nil
		} else {
			return nil, fmt.Errorf("failed to validate postgres: %w", err)
		}
	}

	slog.Info("Scanning table",
		slog.String("tableName", postgresTable.Name),
		slog.String("schemaName", postgresTable.Schema),
		slog.String("topicSuffix", postgresTable.TopicSuffix()),
		slog.Any("primaryKeyColumns", postgresTable.PrimaryKeys.Keys()),
		slog.Any("batchSize", table.GetLimit()),
	)

	return &TableIterator{
		statsD:        statsD,
		maxRowSize:    maxRowSize,
		postgresTable: postgresTable,
		scanner:       NewScanner(db, postgresTable, table.GetLimit(), defaultErrorRetries),
	}, nil
}

func (i *TableIterator) HasNext() bool {
	return i != nil && i.scanner.HasNext()
}

func (i *TableIterator) recordMetrics(start time.Time) {
	if i.statsD != nil {
		(*i.statsD).Timing("scanned_and_parsed", time.Since(start), map[string]string{
			"table":  strings.ReplaceAll(i.postgresTable.Name, `"`, ``),
			"schema": i.postgresTable.Schema,
		})
	}
}

func (i *TableIterator) Next() ([]lib.RawMessage, error) {
	if !i.HasNext() {
		return make([]lib.RawMessage, 0), nil
	}

	rows, err := i.scanner.Next()
	if err != nil {
		return nil, fmt.Errorf("failed to scan postgres: %w", err)
	}

	var result []lib.RawMessage
	for _, row := range rows {
		start := time.Now()
		partitionKeyMap := make(map[string]interface{})
		for _, key := range i.postgresTable.PrimaryKeys.Keys() {
			partitionKeyMap[key] = row[key]
		}

		if i.maxRowSize > 0 {
			if uint64(size.GetApproxSize(row)) > i.maxRowSize {
				slog.Info(fmt.Sprintf("Row greater than %v mb, skipping...", i.maxRowSize/1024/1024), slog.Any("key", partitionKeyMap))
				continue
			}
		}

		payload, err := debezium.NewPayload(&debezium.NewArgs{
			TableName: i.postgresTable.Name,
			Columns:   i.postgresTable.OriginalColumns,
			Fields:    i.postgresTable.Config.Fields,
			RowData:   row,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to generate payload: %w", err)
		}

		result = append(result, lib.RawMessage{
			TopicSuffix:  i.postgresTable.TopicSuffix(),
			PartitionKey: partitionKeyMap,
			Payload:      payload,
		})
		i.recordMetrics(start)
	}
	return result, nil
}
