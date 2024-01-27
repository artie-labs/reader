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

const DefaultErrorRetries = 10

type TableIterator struct {
	db            *sql.DB
	batchSize     uint
	statsD        *mtr.Client
	maxRowSize    uint64
	postgresTable *Table
	firstRow      bool
	lastRow       bool
	done          bool
}

func LoadTable(db *sql.DB, table *config.PostgreSQLTable, statsD *mtr.Client, maxRowSize uint64) (TableIterator, error) {
	slog.Info("Loading configuration for table", slog.String("table", table.Name))

	postgresTable := NewTable(table)
	if err := postgresTable.RetrieveColumns(db); err != nil {
		if NoRowsError(err) {
			slog.Info("Table does not contain any rows, skipping...", slog.String("table", table.Name))
			return TableIterator{done: true}, nil
		} else {
			return TableIterator{done: true}, fmt.Errorf("failed to validate postgres: %w", err)
		}
	}

	slog.Info("Scanning table",
		slog.String("tableName", postgresTable.Name),
		slog.String("schemaName", postgresTable.Schema),
		slog.String("topicSuffix", postgresTable.TopicSuffix()),
		slog.Any("primaryKeyColumns", postgresTable.PrimaryKeys.Keys()),
		slog.Any("batchSize", table.GetLimit()),
	)

	return TableIterator{
		db:            db,
		batchSize:     table.GetLimit(),
		statsD:        statsD,
		maxRowSize:    maxRowSize,
		postgresTable: postgresTable,
		firstRow:      true,
	}, nil
}

func (i *TableIterator) HasNext() bool {
	return !i.done
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
	if i.done {
		return nil, fmt.Errorf("cannot call Next() on a closed iterator")
	}

	rows, err := i.postgresTable.StartScanning(i.db,
		NewScanningArgs(i.postgresTable.PrimaryKeys, i.batchSize, DefaultErrorRetries, i.firstRow, i.lastRow))
	if err != nil {
		return nil, fmt.Errorf("failed to scan postgres: %w", err)
	} else if len(rows) == 0 {
		slog.Info("Finished scanning", slog.String("table", i.postgresTable.Name))
		i.done = true
		return make([]lib.RawMessage, 0), nil
	}

	i.firstRow = false
	// If the number of rows returned is less than the batch size, we've reached the end of the table
	i.lastRow = i.batchSize > uint(len(rows))

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
