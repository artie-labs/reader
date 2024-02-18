package transformer

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/reader/lib/postgres"
)

type DebeziumTransformer struct {
	statsD mtr.Client
	table  *postgres.Table
	iter   batchRowIterator
}

func NewDebeziumTransformer(table *postgres.Table, iter batchRowIterator, statsD mtr.Client) *DebeziumTransformer {
	return &DebeziumTransformer{
		table:  table,
		iter:   iter,
		statsD: statsD,
	}
}

type batchRowIterator interface {
	HasNext() bool
	Next() ([]map[string]interface{}, error)
}

func (m *DebeziumTransformer) HasNext() bool {
	return m != nil && m.iter.HasNext()
}

func (m *DebeziumTransformer) recordMetrics(start time.Time) {
	m.statsD.Timing("scanned_and_parsed", time.Since(start), map[string]string{
		"table":  strings.ReplaceAll(m.table.Name, `"`, ``),
		"schema": m.table.Schema,
	})

}

func (m *DebeziumTransformer) Next() ([]lib.RawMessage, error) {
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

		result = append(result, lib.NewRawMessage(m.table.TopicSuffix(), m.partitionKey(row), payload))
		m.recordMetrics(start)
	}
	return result, nil
}

// PartitionKey returns a map of primary keys and their values for a given row.
func (m *DebeziumTransformer) partitionKey(row map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for _, key := range m.table.PrimaryKeys.Keys() {
		result[key] = row[key]
	}
	return result
}

func (m *DebeziumTransformer) convertRowToDebezium(row map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for key, value := range row {
		col, err := m.table.GetColumnByName(key)
		if err != nil {
			return nil, fmt.Errorf("failed to get column %s by name: %w", key, err)
		}

		val, err := ParseValue(*col, value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert value: %w", err)
		}

		result[key] = val
	}
	return result, nil
}

func (m *DebeziumTransformer) createPayload(row map[string]interface{}) (util.SchemaEventPayload, error) {
	dbzRow, err := m.convertRowToDebezium(row)
	if err != nil {
		return util.SchemaEventPayload{}, fmt.Errorf("failed to convert row to debezium: %w", err)
	}

	schema := debezium.Schema{
		FieldsObject: []debezium.FieldsObject{{
			Fields:     ColumnsToFields(m.table.Columns),
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
