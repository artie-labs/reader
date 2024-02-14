package postgres

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/cdc/util"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/stretchr/testify/assert"
)

type ErrorRowIterator struct{}

func (m *ErrorRowIterator) HasNext() bool {
	return true
}

func (m *ErrorRowIterator) Next() ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("mock error")
}

type MockRowIterator struct {
	batches [][]map[string]interface{}
	index   int
}

func (m *MockRowIterator) HasNext() bool {
	return m.index < len(m.batches)
}

func (m *MockRowIterator) Next() ([]map[string]interface{}, error) {
	result := m.batches[m.index]
	m.index++
	return result, nil
}

func TestMessageBuilder(t *testing.T) {
	table := NewTable(&config.PostgreSQLTable{
		Name:   "table",
		Schema: "schema",
	})
	table.OriginalColumns = []string{"a"}
	table.ColumnsCastedForScanning = []string{"a"}
	table.PrimaryKeys.Upsert("a", ptr.ToString("1"), ptr.ToString("4"))

	// test zero batches
	{
		builder := NewMessageBuilder(
			table,
			&MockRowIterator{batches: [][]map[string]interface{}{}},
			nil,
		)
		assert.False(t, builder.HasNext())
	}

	// test an iterator that returns an error
	{
		builder := NewMessageBuilder(
			table,
			&ErrorRowIterator{},
			nil,
		)

		assert.True(t, builder.HasNext())
		_, err := builder.Next()
		assert.ErrorContains(t, err, "mock error")
	}

	// test two batches each with two rows
	{
		builder := NewMessageBuilder(
			table,
			&MockRowIterator{
				batches: [][]map[string]interface{}{
					{{"a": "1", "b": "11"}, {"a": "2", "b": "12"}},
					{{"a": "3", "b": "13"}, {"a": "4", "b": "14"}},
				},
			},
			nil,
		)

		assert.True(t, builder.HasNext())
		msgs1, err := builder.Next()
		assert.NoError(t, err)
		assert.Len(t, msgs1, 2)
		assert.Equal(t, "schema.table", msgs1[0].TopicSuffix)
		assert.Equal(t, map[string]interface{}{"a": "1"}, msgs1[0].PartitionKey)
		assert.Equal(t, map[string]interface{}{"a": "1", "b": "11"}, msgs1[0].GetPayload().(util.SchemaEventPayload).Payload.After)
		assert.Equal(t, "schema.table", msgs1[1].TopicSuffix)
		assert.Equal(t, map[string]interface{}{"a": "2"}, msgs1[1].PartitionKey)
		assert.Equal(t, map[string]interface{}{"a": "2", "b": "12"}, msgs1[1].GetPayload().(util.SchemaEventPayload).Payload.After)

		assert.True(t, builder.HasNext())
		msgs2, err := builder.Next()
		assert.NoError(t, err)
		assert.Len(t, msgs2, 2)
		assert.Equal(t, "schema.table", msgs2[0].TopicSuffix)
		assert.Equal(t, map[string]interface{}{"a": "3"}, msgs2[0].PartitionKey)
		assert.Equal(t, map[string]interface{}{"a": "3", "b": "13"}, msgs2[0].GetPayload().(util.SchemaEventPayload).Payload.After)
		assert.Equal(t, "schema.table", msgs2[1].TopicSuffix)
		assert.Equal(t, map[string]interface{}{"a": "4"}, msgs2[1].PartitionKey)
		assert.Equal(t, map[string]interface{}{"a": "4", "b": "14"}, msgs2[1].GetPayload().(util.SchemaEventPayload).Payload.After)

		assert.False(t, builder.HasNext())
	}
}
