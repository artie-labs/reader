package postgres

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/reader/lib/postgres/debezium"
	"github.com/artie-labs/transfer/lib/size"
)

type MessageBuilder struct {
	statsD     *mtr.Client
	maxRowSize uint64
	table      *Table
	iter       batchRowIterator
}

type batchRowIterator interface {
	HasNext() bool
	Next() ([]map[string]interface{}, error)
}

func NewMessageBuilder(table *Table, iter batchRowIterator, statsD *mtr.Client, maxRowSize uint64) *MessageBuilder {
	return &MessageBuilder{
		table:      table,
		iter:       iter,
		statsD:     statsD,
		maxRowSize: maxRowSize,
	}
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
		if m.maxRowSize > 0 && uint64(size.GetApproxSize(row)) > m.maxRowSize {
			slog.Info(fmt.Sprintf("Row greater than %v mb, skipping...", m.maxRowSize/1024/1024), slog.Any("key", m.table.PartitionKey(row)))
			continue
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

		result = append(result, lib.NewRawMessage(m.table.TopicSuffix(), m.table.PartitionKey(row), payload))
		m.recordMetrics(start)
	}
	return result, nil
}
