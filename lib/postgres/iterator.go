package postgres

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/reader/lib/postgres/debezium"
	"github.com/artie-labs/transfer/lib/size"
)

const DefaultErrorRetries = 10

type TableIterator struct {
	db            *sql.DB
	limit         uint
	statsD        *mtr.Client
	maxRowSize    uint64
	postgresTable *Table
	firstRow      bool
	lastRow       bool
	done          bool
}

func LoadTable(db *sql.DB, table *config.PostgreSQLTable, statsD *mtr.Client, maxRowSize uint64) (TableIterator, error) {
	slog.Info("Loading configuration for table", slog.String("table", table.Name), slog.Any("limitSize", table.GetLimit()))

	postgresTable := NewTable(table)
	err := postgresTable.RetrieveColumns(db)
	if err != nil {
		if NoRowsError(err) {
			slog.Info("Table does not contain any rows, skipping...", slog.String("table", table.Name))
			return TableIterator{}, nil
		} else {
			return TableIterator{}, fmt.Errorf("failed to validate postgres: %w", err)
		}
	}

	slog.Info("Scanning table",
		slog.String("topicSuffix", postgresTable.TopicSuffix()),
		slog.String("tableName", postgresTable.Name),
		slog.String("schemaName", postgresTable.Schema),
		slog.Any("primaryKeyColumns", postgresTable.PrimaryKeys.Keys()),
	)

	return TableIterator{
		db:            db,
		limit:         table.GetLimit(),
		statsD:        statsD,
		maxRowSize:    maxRowSize,
		postgresTable: postgresTable,
		firstRow:      true,
	}, nil
}

func (i *TableIterator) HasNext() bool {
	return !i.done
}

func (i *TableIterator) statsDTags() map[string]string {
	return map[string]string{
		"table":  strings.ReplaceAll(i.postgresTable.Name, `"`, ``),
		"schema": i.postgresTable.Schema,
	}
}

func (i *TableIterator) Next() ([]kafkalib.RawMessage, error) {
	var rows []map[string]interface{}
	rows, err := i.postgresTable.StartScanning(i.db,
		NewScanningArgs(i.postgresTable.PrimaryKeys, i.limit, DefaultErrorRetries, i.firstRow, i.lastRow))
	if err != nil {
		return nil, fmt.Errorf("failed to scan postgres: %w", err)
	}

	i.firstRow = false
	var msgs []kafkalib.RawMessage
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

		msgs = append(msgs, kafkalib.RawMessage{
			TopicSuffix:  i.postgresTable.TopicSuffix(),
			PartitionKey: partitionKeyMap,
			Payload:      payload,
		})
		if i.statsD != nil {
			(*i.statsD).Timing("scanned_and_parsed", time.Since(start), i.statsDTags())
		}
	}

	// TODO: This should really be re-written and tested thoroughly
	// It's super confusing to read.
	if i.limit > uint(len(rows)) {
		if len(rows) == 0 {
			slog.Info("Finished scanning, exiting...", slog.Int("rows", len(rows)))
			i.done = true
		} else {
			i.lastRow = true
		}
	} else {
		i.lastRow = false
	}

	return msgs, nil
}
