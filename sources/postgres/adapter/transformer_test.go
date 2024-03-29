package adapter

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/debezium/converters"
	"github.com/artie-labs/reader/lib/debezium/transformer"
	"github.com/artie-labs/reader/lib/postgres"
	"github.com/artie-labs/reader/lib/postgres/schema"
)

type ErrorRowIterator struct{}

func (m *ErrorRowIterator) HasNext() bool {
	return true
}

func (m *ErrorRowIterator) Next() ([]map[string]any, error) {
	return nil, fmt.Errorf("mock error")
}

type MockRowIterator struct {
	batches [][]map[string]any
	index   int
}

func (m *MockRowIterator) HasNext() bool {
	return m.index < len(m.batches)
}

func (m *MockRowIterator) Next() ([]map[string]any, error) {
	result := m.batches[m.index]
	m.index++
	return result, nil
}

func TestDebeziumTransformer(t *testing.T) {
	table := postgres.Table{
		Schema: "schema",
		Name:   "table",
		Columns: []schema.Column{
			{Name: "a", Type: schema.Int16},
			{Name: "b", Type: schema.Text},
		},
		PrimaryKeys: []string{"a"},
	}

	// test zero batches
	{
		dbzTransformer := transformer.NewDebeziumTransformerWithIterator(
			PostgresAdapter{table: table},
			&MockRowIterator{batches: [][]map[string]any{}},
		)
		assert.False(t, dbzTransformer.HasNext())
	}

	// test an iterator that returns an error
	{
		dbzTransformer := transformer.NewDebeziumTransformerWithIterator(
			PostgresAdapter{table: table},
			&ErrorRowIterator{},
		)

		assert.True(t, dbzTransformer.HasNext())
		_, err := dbzTransformer.Next()
		assert.ErrorContains(t, err, "mock error")
	}

	// test two batches each with two rows
	{
		dbzTransformer := transformer.NewDebeziumTransformerWithIterator(
			PostgresAdapter{
				table: table,
				fieldConverters: []transformer.FieldConverter{
					{Name: "a", ValueConverter: converters.StringPassthrough{}},
					{Name: "b", ValueConverter: converters.StringPassthrough{}},
				},
			},
			&MockRowIterator{
				batches: [][]map[string]any{
					{{"a": "1", "b": "11"}, {"a": "2", "b": "12"}},
					{{"a": "3", "b": "13"}, {"a": "4", "b": "14"}},
				},
			},
		)

		assert.True(t, dbzTransformer.HasNext())
		msgs1, err := dbzTransformer.Next()
		assert.NoError(t, err)
		assert.Len(t, msgs1, 2)
		assert.Equal(t, "schema.table", msgs1[0].TopicSuffix)
		assert.Equal(t, map[string]any{"a": "1"}, msgs1[0].PartitionKey)
		assert.Equal(t, map[string]any{"a": "1", "b": "11"}, msgs1[0].GetPayload().(util.SchemaEventPayload).Payload.After)
		assert.Equal(t, "schema.table", msgs1[1].TopicSuffix)
		assert.Equal(t, map[string]any{"a": "2"}, msgs1[1].PartitionKey)
		assert.Equal(t, map[string]any{"a": "2", "b": "12"}, msgs1[1].GetPayload().(util.SchemaEventPayload).Payload.After)

		assert.True(t, dbzTransformer.HasNext())
		msgs2, err := dbzTransformer.Next()
		assert.NoError(t, err)
		assert.Len(t, msgs2, 2)
		assert.Equal(t, "schema.table", msgs2[0].TopicSuffix)
		assert.Equal(t, map[string]any{"a": "3"}, msgs2[0].PartitionKey)
		assert.Equal(t, map[string]any{"a": "3", "b": "13"}, msgs2[0].GetPayload().(util.SchemaEventPayload).Payload.After)
		assert.Equal(t, "schema.table", msgs2[1].TopicSuffix)
		assert.Equal(t, map[string]any{"a": "4"}, msgs2[1].PartitionKey)
		assert.Equal(t, map[string]any{"a": "4", "b": "14"}, msgs2[1].GetPayload().(util.SchemaEventPayload).Payload.After)

		assert.False(t, dbzTransformer.HasNext())
	}
}

func TestDebeziumTransformer_NilOptionalSchema(t *testing.T) {
	table := postgres.Table{
		Schema: "schema",
		Name:   "foo",
		Columns: []schema.Column{
			{Name: "user_id", Type: schema.Int16},
			{Name: "name", Type: schema.Text},
		},
	}

	rowData := map[string]any{
		"user_id": int16(123),
		"name":    "Robin",
	}

	dbzTransformer := transformer.NewDebeziumTransformerWithIterator(
		PostgresAdapter{
			table: table,
			fieldConverters: []transformer.FieldConverter{
				{Name: "user_id", ValueConverter: converters.Int16Passthrough{}},
				{Name: "name", ValueConverter: converters.StringPassthrough{}},
			},
		},
		&MockRowIterator{batches: [][]map[string]any{{rowData}}},
	)

	rows, err := dbzTransformer.Next()
	assert.NoError(t, err)
	assert.NotNil(t, rows)
	payload := rows[0].GetPayload().(util.SchemaEventPayload)

	assert.Equal(t, "r", payload.Payload.Operation)
	assert.Equal(t, rowData, payload.Payload.After)
	assert.Equal(t, "foo", payload.GetTableName())
}
