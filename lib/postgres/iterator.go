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

type MessageBuilder struct {
	statsD     *mtr.Client
	maxRowSize uint64
	table      *Table
	iter       batchRowIterator
}

func NewMessageBuilder(table *Table, iter batchRowIterator, statsD *mtr.Client, maxRowSize uint64) *MessageBuilder {
	return &MessageBuilder{
		table:      table,
		iter:       iter,
		statsD:     statsD,
		maxRowSize: maxRowSize,
	}
}

type batchRowIterator interface {
	HasNext() bool
	Next() ([]map[string]interface{}, error)
}

func LoadTable(db *sql.DB, tableCfg *config.PostgreSQLTable, statsD *mtr.Client, maxRowSize uint64) (*MessageBuilder, error) {
	slog.Info("Loading configuration for table", slog.String("table", tableCfg.Name))

	table := NewTable(tableCfg)
	if err := table.RetrieveColumns(db); err != nil {
		if NoRowsError(err) {
			slog.Info("Table does not contain any rows, skipping...", slog.String("table", table.Name))
			return nil, nil
		} else {
			return nil, fmt.Errorf("failed to validate postgres: %w", err)
		}
	}

	slog.Info("Scanning table",
		slog.String("tableName", table.Name),
		slog.String("schemaName", table.Schema),
		slog.String("topicSuffix", table.TopicSuffix()),
		slog.Any("primaryKeyColumns", table.PrimaryKeys.Keys()),
		slog.Any("batchSize", tableCfg.GetLimit()),
	)

	scanner := table.NewScanner(db, tableCfg.GetLimit(), defaultErrorRetries)
	return NewMessageBuilder(
		table,
		&scanner,
		statsD,
		maxRowSize,
	), nil
}

func (m *MessageBuilder) HasNext() bool {
	return m != nil && m.iter.HasNext()
}

func (m *MessageBuilder) recordMetrics(start time.Time) {
	if m.statsD != nil {
		(*m.statsD).Timing("scanned_and_parsed", time.Since(start), map[string]string{
			"table":  strings.ReplaceAll(m.table.Name, `"`, ``),
			"schema": m.table.Schema,
		})
	}
}

func (m *MessageBuilder) Next() ([]lib.RawMessage, error) {
	if !m.HasNext() {
		return make([]lib.RawMessage, 0), nil
	}

	rows, err := m.iter.Next()
	if err != nil {
		return nil, fmt.Errorf("failed to scan postgres: %w", err)
	}

	var result []lib.RawMessage
	for _, row := range rows {
		start := time.Now()
		partitionKeyMap := make(map[string]interface{})
		for _, key := range m.table.PrimaryKeys.Keys() {
			partitionKeyMap[key] = row[key]
		}

		if m.maxRowSize > 0 {
			if uint64(size.GetApproxSize(row)) > m.maxRowSize {
				slog.Info(fmt.Sprintf("Row greater than %v mb, skipping...", m.maxRowSize/1024/1024), slog.Any("key", partitionKeyMap))
				continue
			}
		}

		payload, err := debezium.NewPayload(&debezium.NewArgs{
			TableName: m.table.Name,
			Columns:   m.table.OriginalColumns,
			Fields:    m.table.Config.Fields,
			RowData:   row,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create debezium payload: %w", err)
		}

		result = append(result, lib.RawMessage{
			TopicSuffix:  m.table.TopicSuffix(),
			PartitionKey: partitionKeyMap,
			Payload:      payload,
		})
		m.recordMetrics(start)
	}
	return result, nil
}
