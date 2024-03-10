package transformer

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/debezium/converters"
)

type MockConverter struct {
	intConverter bool
}

func (m MockConverter) ToField(name string) debezium.Field {
	if m.intConverter {
		return converters.Int32Passthrough{}.ToField(name)
	} else {
		return converters.StringPassthrough{}.ToField(name)
	}
}

func (MockConverter) Convert(value any) (any, error) {
	return fmt.Sprintf("converted-%v", value), nil
}

type mockAdatper struct {
	partitionKeys   []string
	fieldConverters []FieldConverter
	iter            RowsIterator
}

func (m mockAdatper) TableName() string {
	return "im-a-little-table"
}

func (m mockAdatper) TopicSuffix() string {
	return "im-a-little-topic-suffix"
}

func (m mockAdatper) PartitionKeys() []string {
	return m.partitionKeys
}

func (m mockAdatper) FieldConverters() []FieldConverter {
	return m.fieldConverters
}

func (m mockAdatper) NewIterator() (RowsIterator, error) {
	return m.iter, nil
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
		transformer, err := NewDebeziumTransformer(mockAdatper{iter: &mockIterator{}})
		assert.NoError(t, err)
		assert.False(t, transformer.HasNext())
		rows, err := transformer.Next()
		assert.NoError(t, err)
		assert.Len(t, rows, 0)
	}
	{
		// One empty batch
		batches := [][]map[string]any{{}}
		transformer, err := NewDebeziumTransformer(mockAdatper{iter: &mockIterator{batches: batches}})
		assert.NoError(t, err)
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
		fieldConverters := []FieldConverter{
			{Name: "foo", ValueConverter: MockConverter{intConverter: false}},
			{Name: "qux", ValueConverter: MockConverter{intConverter: true}},
		}
		batches := [][]map[string]any{{
			{"foo": "bar", "qux": "quux"},
		}}
		transformer, err := NewDebeziumTransformer(mockAdatper{fieldConverters: fieldConverters, iter: &mockIterator{batches: batches}})
		assert.NoError(t, err)
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
		fieldConverters := []FieldConverter{
			{Name: "foo", ValueConverter: MockConverter{}},
			{Name: "qux", ValueConverter: MockConverter{}},
			{Name: "corge", ValueConverter: MockConverter{}},
			{Name: "garply", ValueConverter: MockConverter{}},
		}
		batches := [][]map[string]any{
			{
				{"foo": "bar", "qux": "quux"},
			},
			{},
			{
				{"corge": "grault", "garply": "waldo"},
			},
		}
		transformer, err := NewDebeziumTransformer(mockAdatper{fieldConverters: fieldConverters, iter: &mockIterator{batches: batches}})
		assert.NoError(t, err)
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
	fieldConverters := []FieldConverter{
		{Name: "foo", ValueConverter: MockConverter{intConverter: false}},
		{Name: "qux", ValueConverter: MockConverter{intConverter: true}},
		{Name: "baz", ValueConverter: MockConverter{intConverter: false}},
	}
	batches := [][]map[string]any{{
		{"foo": "bar", "qux": 12, "baz": "corge"},
	}}
	transformer, err := NewDebeziumTransformer(
		mockAdatper{fieldConverters: fieldConverters, partitionKeys: []string{"foo", "qux"}, iter: &mockIterator{batches: batches}},
	)
	assert.NoError(t, err)
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
						Fields: []debezium.Field{
							{FieldName: "foo", Type: "string"},
							{FieldName: "qux", Type: "int32"},
							{FieldName: "baz", Type: "string"},
						},
						Optional:   false,
						FieldLabel: "after",
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
	fieldConverters := []FieldConverter{
		{Name: "foo", ValueConverter: MockConverter{intConverter: false}},
		{Name: "qux", ValueConverter: MockConverter{intConverter: true}},
	}

	transformer, err := NewDebeziumTransformer(mockAdatper{fieldConverters: fieldConverters, iter: &mockIterator{}})
	assert.NoError(t, err)
	payload, err := transformer.createPayload(map[string]any{"foo": "bar", "qux": "quux"})
	assert.NoError(t, err)
	payload.Payload.Source.TsMs = 12345 // Modify source time since it'll be ~now
	expected := util.SchemaEventPayload(
		util.SchemaEventPayload{
			Schema: debezium.Schema{
				SchemaType: "",
				FieldsObject: []debezium.FieldsObject{
					{
						Fields:     []debezium.Field{{FieldName: "foo", Type: "string"}, {FieldName: "qux", Type: "int32"}},
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

func TestDebeziumTransformer_PartitionKey(t *testing.T) {
	type _tc struct {
		name     string
		keys     []string
		row      map[string]any
		expected map[string]any
	}

	testCases := []_tc{
		{
			name:     "no primary keys",
			row:      map[string]any{},
			expected: map[string]any{},
		},
		{
			name:     "primary keys - empty row",
			keys:     []string{"foo", "bar"},
			row:      map[string]any{},
			expected: map[string]any{"foo": nil, "bar": nil},
		},
		{
			name:     "primary keys - row has data",
			keys:     []string{"foo", "bar"},
			row:      map[string]any{"foo": "a", "bar": 2, "baz": 3},
			expected: map[string]any{"foo": "a", "bar": 2},
		},
	}

	for _, testCase := range testCases {
		transformer, err := NewDebeziumTransformer(mockAdatper{partitionKeys: testCase.keys})
		assert.NoError(t, err)
		assert.Equal(t, testCase.expected, transformer.partitionKey(testCase.row), testCase.name)
	}
}
