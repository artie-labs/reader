package postgres

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/reader/lib/postgres/debezium"
)

type MessageBuilder struct {
	statsD mtr.Client
	table  *Table
	iter   batchRowIterator
}

func NewMessageBuilder(table *Table, iter batchRowIterator, statsD mtr.Client) *MessageBuilder {
	return &MessageBuilder{
		table:  table,
		iter:   iter,
		statsD: statsD,
	}
}

type batchRowIterator interface {
	HasNext() bool
	Next() ([]map[string]interface{}, error)
}

func (m *MessageBuilder) HasNext() bool {
	return m != nil && m.iter.HasNext()
}

func (m *MessageBuilder) recordMetrics(start time.Time) {
	m.statsD.Timing("scanned_and_parsed", time.Since(start), map[string]string{
		"table":  strings.ReplaceAll(m.table.Name, `"`, ``),
		"schema": m.table.Schema,
	})

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
		payload, err := debezium.NewPayload(&debezium.NewArgs{
			TableName: m.table.Name,
			Fields:    m.table.Fields,
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
