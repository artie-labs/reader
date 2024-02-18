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

func (d *DebeziumTransformer) HasNext() bool {
	return d != nil && d.iter.HasNext()
}

func (d *DebeziumTransformer) recordMetrics(start time.Time) {
	d.statsD.Timing("scanned_and_parsed", time.Since(start), map[string]string{
		"table":  strings.ReplaceAll(d.table.Name, `"`, ``),
		"schema": d.table.Schema,
	})

}

func (d *DebeziumTransformer) Next() ([]lib.RawMessage, error) {
	if !d.HasNext() {
		return make([]lib.RawMessage, 0), nil
	}

	rows, err := d.iter.Next()
	if err != nil {
		return nil, fmt.Errorf("failed to scan postgres: %w", err)
	}

	var result []lib.RawMessage
	for _, row := range rows {
		start := time.Now()

		payload, err := d.createPayload(row)
		if err != nil {
			return nil, fmt.Errorf("failed to create debezium payload: %w", err)
		}

		result = append(result, lib.NewRawMessage(d.topicSuffix(), d.partitionKey(row), payload))
		d.recordMetrics(start)
	}
	return result, nil
}

func (d *DebeziumTransformer) topicSuffix() string {
	return fmt.Sprintf("%s.%s", d.table.Schema, strings.ReplaceAll(d.table.Name, `"`, ``))
}

// partitionKey returns a map of primary keys and their values for a given row.
func (d *DebeziumTransformer) partitionKey(row map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for _, key := range d.table.PrimaryKeys.Keys() {
		result[key] = row[key]
	}
	return result
}

func (d *DebeziumTransformer) convertRowToDebezium(row map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for key, value := range row {
		col, err := d.table.GetColumnByName(key)
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

func (d *DebeziumTransformer) createPayload(row map[string]interface{}) (util.SchemaEventPayload, error) {
	dbzRow, err := d.convertRowToDebezium(row)
	if err != nil {
		return util.SchemaEventPayload{}, fmt.Errorf("failed to convert row to debezium: %w", err)
	}

	schema := debezium.Schema{
		FieldsObject: []debezium.FieldsObject{{
			Fields:     ColumnsToFields(d.table.Columns),
			Optional:   false,
			FieldLabel: cdc.After,
		}},
	}

	payload := util.Payload{
		After: dbzRow,
		Source: util.Source{
			Table: d.table.Name,
			TsMs:  time.Now().UnixMilli(),
		},
		Operation: "r",
	}

	return util.SchemaEventPayload{
		Schema:  schema,
		Payload: payload,
	}, nil
}
