package debezium

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/stretchr/testify/assert"
)

type mockAdatper struct {
	partitionKeys []string
	fields        []debezium.Field
}

func (m mockAdatper) TableName() string {
	return "im-a-little-table"
}

func (m mockAdatper) TopicSuffix() string {
	return "im-a-little-topic-suffix"
}

func (m mockAdatper) PartitionKey(row map[string]any) map[string]any {
	result := map[string]any{}
	for _, key := range m.partitionKeys {
		result[key] = row[key]
	}
	return result
}

func (m mockAdatper) Fields() []debezium.Field {
	return m.fields
}

func (m mockAdatper) NewIterator() (RowsIterator, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m mockAdatper) ConvertRowToDebezium(row map[string]any) (map[string]any, error) {
	newRow := make(map[string]any)
	for key, value := range row {
		newRow[key] = fmt.Sprintf("converted-%v", value)
	}
	return newRow, nil
}

type mockIterator struct {
	index   int
	batches [][]map[string]any
}

func (m *mockIterator) HasNext() bool {
	return m.index < len(m.batches)
}

func (m *mockIterator) Next() ([]map[string]any, error) {
	if !m.HasNext() {
		return nil, fmt.Errorf("done")
	}
	result := m.batches[m.index]
	m.index++
	return result, nil
}

func TestDebeziumTransformer_Iteration(t *testing.T) {
	{
		// Empty iterator
		transformer := NewDebeziumTransformer(mockAdatper{}, &mockIterator{})
		assert.False(t, transformer.HasNext())
		rows, err := transformer.Next()
		assert.NoError(t, err)
		assert.Len(t, rows, 0)
	}
	{
		// One empty batch
		batches := [][]map[string]any{{}}
		transformer := NewDebeziumTransformer(mockAdatper{}, &mockIterator{batches: batches})
		assert.True(t, transformer.HasNext())
		rows, err := transformer.Next()
		assert.NoError(t, err)
		assert.Len(t, rows, 0)
		assert.False(t, transformer.HasNext())
		// Subsequent calls to `.Next()` should be empty
		rows, err = transformer.Next()
		assert.NoError(t, err)
		assert.Len(t, rows, 0)
	}
	{
		// One non-empty batch
		batches := [][]map[string]any{{
			{"foo": "bar", "qux": "quux"},
		}}
		transformer := NewDebeziumTransformer(mockAdatper{}, &mockIterator{batches: batches})
		// First batch
		assert.True(t, transformer.HasNext())
		rows, err := transformer.Next()
		assert.NoError(t, err)
		assert.Len(t, rows, 1)
		payload, isOk := rows[0].GetPayload().(util.SchemaEventPayload)
		assert.True(t, isOk)
		assert.Equal(t, "converted-bar", payload.Payload.After["foo"])
		// Second batch
		assert.False(t, transformer.HasNext())
		// Subsequent calls to `.Next()` should be empty
		rows, err = transformer.Next()
		assert.NoError(t, err)
		assert.Len(t, rows, 0)
	}
	{
		// Two non-empty batches, one empty batch
		batches := [][]map[string]any{
			{
				{"foo": "bar", "qux": "quux"},
			},
			{},
			{
				{"corge": "grault", "garply": "waldo"},
			},
		}
		transformer := NewDebeziumTransformer(mockAdatper{}, &mockIterator{batches: batches})
		// First batch
		assert.True(t, transformer.HasNext())
		rows, err := transformer.Next()
		assert.NoError(t, err)
		assert.Len(t, rows, 1)
		payload, isOk := rows[0].GetPayload().(util.SchemaEventPayload)
		assert.True(t, isOk)
		assert.Equal(t, "converted-bar", payload.Payload.After["foo"])
		// Second batch
		assert.True(t, transformer.HasNext())
		rows, err = transformer.Next()
		assert.NoError(t, err)
		assert.Len(t, rows, 0)
		// Third batch
		assert.True(t, transformer.HasNext())
		rows, err = transformer.Next()
		assert.NoError(t, err)
		assert.Len(t, rows, 1)
		payload, isOk = rows[0].GetPayload().(util.SchemaEventPayload)
		assert.True(t, isOk)
		assert.Equal(t, "converted-grault", payload.Payload.After["corge"])
		// Subsequent calls to `.Next()` should be empty
		rows, err = transformer.Next()
		assert.NoError(t, err)
		assert.Len(t, rows, 0)
	}
}

func TestDebeziumTransformer_Next(t *testing.T) {
	fields := []debezium.Field{
		{Type: "string"},
		{Type: "int"},
	}
	batches := [][]map[string]any{{
		{"foo": "bar", "qux": 12, "baz": "corge"},
	}}
	transformer := NewDebeziumTransformer(
		mockAdatper{fields: fields, partitionKeys: []string{"foo", "qux"}},
		&mockIterator{batches: batches},
	)
	assert.True(t, transformer.HasNext())
	rows, err := transformer.Next()
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	rawMessage := rows[0]
	assert.Equal(t, map[string]any{"foo": "bar", "qux": 12}, rawMessage.PartitionKey)
	assert.Equal(t, "im-a-little-topic-suffix", rawMessage.TopicSuffix)
	payload, isOk := rawMessage.GetPayload().(util.SchemaEventPayload)
	assert.True(t, isOk)
	payload.Payload.Source.TsMs = 12345 // Modify source time since it'll be ~now
	expected := util.SchemaEventPayload(
		util.SchemaEventPayload{
			Schema: debezium.Schema{
				SchemaType: "",
				FieldsObject: []debezium.FieldsObject{
					{
						FieldObjectType: "",
						Fields:          []debezium.Field{{Type: "string"}, {Type: "int"}},
						Optional:        false,
						FieldLabel:      "after",
					},
				},
			},
			Payload: util.Payload{
				After:     map[string]interface{}{"foo": "converted-bar", "qux": "converted-12", "baz": "converted-corge"},
				Source:    util.Source{Connector: "", TsMs: 12345, Database: "", Schema: "", Table: "im-a-little-table"},
				Operation: "r",
			},
		},
	)
	assert.Equal(t, expected, payload)
}

func TestDebeziumTransformer_CreatePayload(t *testing.T) {
	fields := []debezium.Field{
		{Type: "string"},
		{Type: "int"},
	}

	transformer := NewDebeziumTransformer(mockAdatper{fields: fields}, nil)
	payload, err := transformer.createPayload(map[string]any{"foo": "bar", "qux": "quux"})
	assert.NoError(t, err)
	payload.Payload.Source.TsMs = 12345 // Modify source time since it'll be ~now
	expected := util.SchemaEventPayload(
		util.SchemaEventPayload{
			Schema: debezium.Schema{
				SchemaType: "",
				FieldsObject: []debezium.FieldsObject{
					{
						Fields:     []debezium.Field{{Type: "string"}, {Type: "int"}},
						Optional:   false,
						FieldLabel: "after",
					},
				},
			},
			Payload: util.Payload{
				After:     map[string]interface{}{"foo": "converted-bar", "qux": "converted-quux"},
				Source:    util.Source{TsMs: 12345, Table: "im-a-little-table"},
				Operation: "r",
			},
		},
	)
	assert.Equal(t, expected, payload)
}
