package postgres

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/mtr"
	pgDebezium "github.com/artie-labs/reader/lib/postgres/debezium"
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

		payload, err := m.createPayload(row)
		if err != nil {
			return nil, fmt.Errorf("failed to create debezium payload: %w", err)
		}

		result = append(result, lib.NewRawMessage(m.table.TopicSuffix(), m.table.PartitionKey(row), payload))
		m.recordMetrics(start)
	}
	return result, nil
}

func (m *MessageBuilder) convertRowToDebezium(row map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for key, value := range row {
		col, err := m.table.GetColumnByName(key)
		if err != nil {
			return nil, fmt.Errorf("failed to get column %s by name: %w", key, err)
		}

		val, err := pgDebezium.ParseValue(*col, value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert value: %w", err)
		}

		result[key] = val
	}
	return result, nil
}

func (m *MessageBuilder) createPayload(row map[string]interface{}) (util.SchemaEventPayload, error) {
	dbzRow, err := m.convertRowToDebezium(row)
	if err != nil {
		return util.SchemaEventPayload{}, fmt.Errorf("failed to convert row to debezium: %w", err)
	}

	schema := debezium.Schema{
		FieldsObject: []debezium.FieldsObject{{
			Fields:     pgDebezium.ColumnsToFields(m.table.Columns),
			Optional:   false,
			FieldLabel: cdc.After,
		}},
	}

	payload := util.Payload{
		After: dbzRow,
		Source: util.Source{
			Table: m.table.Name,
			TsMs:  time.Now().UnixMilli(),
		},
		Operation: "r",
	}

	return util.SchemaEventPayload{
		Schema:  schema,
		Payload: payload,
	}, nil
}
