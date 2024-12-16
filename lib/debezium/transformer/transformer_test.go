package transformer

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/debezium/converters"
	"github.com/artie-labs/reader/lib/iterator"
)

func parseUsingTransfer(payload debezium.PrimaryKeyPayload) (map[string]any, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return debezium.ParsePartitionKey(payloadBytes, kafkalib.JSONKeyFmt)
}

func TestConvertPartitionKey(t *testing.T) {
	row := map[string]any{"id": 12, "name": "bar"}
	{
		// If the schema is not provided, it will still parse fine.
		payload := debezium.PrimaryKeyPayload{
			Payload: row,
		}

		val, err := parseUsingTransfer(payload)
		assert.NoError(t, err)
		// This is a float64 since we didn't pass in a schema and JSON unmarshalls numbers as float64.
		assert.Equal(t, map[string]any{"id": float64(12), "name": "bar"}, val)
	}
	{
		// Make sure that we are correctly parsing ints
		valueConverters := map[string]converters.ValueConverter{
			"id":   converters.Int64Passthrough{},
			"name": converters.StringPassthrough{},
		}
		partitionKeys := []string{"id", "name"}
		pkPayload, err := convertPartitionKey(valueConverters, partitionKeys, row)
		assert.NoError(t, err)

		val, err := parseUsingTransfer(pkPayload)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"id": int64(12), "name": "bar"}, val)
	}
}

type testConverter struct {
	intField  bool
	returnErr bool
}

func (t testConverter) ToField(name string) debezium.Field {
	if t.intField {
		return converters.Int32Passthrough{}.ToField(name)
	} else {
		return converters.StringPassthrough{}.ToField(name)
	}
}

func (t testConverter) Convert(value any) (any, error) {
	if t.returnErr {
		return nil, fmt.Errorf("test error")
	}
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

type errorIterator struct{}

func (m *errorIterator) HasNext() bool {
	return true
}

func (m *errorIterator) Next() ([]Row, error) {
	return nil, fmt.Errorf("test iteration error")
}

func TestDebeziumTransformer_Iteration(t *testing.T) {
	{
		// Empty iterator
		transformer, err := NewDebeziumTransformer(mockAdatper{iter: iterator.ForSlice([][]Row{})})
		assert.NoError(t, err)
		items, err := iterator.Collect(transformer)
		assert.NoError(t, err)
		assert.Empty(t, items)
	}
	{
		// One empty batch
		batches := [][]Row{{}}
		transformer, err := NewDebeziumTransformer(mockAdatper{iter: iterator.ForSlice(batches)})
		assert.NoError(t, err)
		results, err := iterator.Collect(transformer)
		assert.NoError(t, err)
		assert.Len(t, results, 1)
		assert.Empty(t, results[0])
	}
	{
		// One non-empty batch
		fieldConverters := []FieldConverter{
			{Name: "foo", ValueConverter: testConverter{intField: false}},
			{Name: "qux", ValueConverter: testConverter{intField: true}},
		}
		batches := [][]Row{{
			{"foo": "bar", "qux": "quux"},
		}}
		transformer, err := NewDebeziumTransformer(mockAdatper{
			fieldConverters: fieldConverters,
			iter:            iterator.ForSlice(batches),
		})
		assert.NoError(t, err)
		results, err := iterator.Collect(transformer)
		assert.NoError(t, err)
		assert.Len(t, results, 1)
		rows := results[0]
		assert.Len(t, rows, 1)
		payload, isOk := rows[0].Event().(*util.SchemaEventPayload)
		assert.True(t, isOk)
		assert.Equal(t, "converted-bar", payload.Payload.After["foo"])
	}
	{
		// Two non-empty batches, one empty batch
		fieldConverters := []FieldConverter{
			{Name: "foo", ValueConverter: testConverter{}},
			{Name: "qux", ValueConverter: testConverter{}},
			{Name: "corge", ValueConverter: testConverter{}},
			{Name: "garply", ValueConverter: testConverter{}},
		}
		batches := [][]Row{
			{
				{"foo": "bar", "qux": "quux"},
			},
			{},
			{
				{"corge": "grault", "garply": "waldo"},
			},
		}
		transformer, err := NewDebeziumTransformer(mockAdatper{
			fieldConverters: fieldConverters,
			iter:            iterator.ForSlice(batches),
		})
		assert.NoError(t, err)
		results, err := iterator.Collect(transformer)
		assert.NoError(t, err)
		assert.Len(t, results, 3)
		// First batch
		rows := results[0]
		assert.Len(t, rows, 1)
		payload, isOk := rows[0].Event().(*util.SchemaEventPayload)
		assert.True(t, isOk)
		assert.Equal(t, "converted-bar", payload.Payload.After["foo"])
		// Second batch
		assert.Empty(t, results[1], 0)
		// Third batch
		rows = results[2]
		assert.Len(t, rows, 1)
		payload, isOk = rows[0].Event().(*util.SchemaEventPayload)
		assert.True(t, isOk)
		assert.Equal(t, "converted-grault", payload.Payload.After["corge"])
	}
}

func TestDebeziumTransformer_Next(t *testing.T) {
	{
		// Iteration error
		fieldConverters := []FieldConverter{{Name: "foo", ValueConverter: testConverter{}}}
		transformer, err := NewDebeziumTransformer(
			mockAdatper{
				fieldConverters: fieldConverters,
				partitionKeys:   []string{"foo"},
				iter:            &errorIterator{},
			},
		)
		assert.NoError(t, err)
		_, err = iterator.Collect(transformer)
		assert.ErrorContains(t, err, `failed to scan: test iteration error`)
	}
	{
		// Value converter error
		fieldConverters := []FieldConverter{
			{Name: "foo", ValueConverter: testConverter{returnErr: true}},
		}
		transformer, err := NewDebeziumTransformer(mockAdatper{
			fieldConverters: fieldConverters,
			partitionKeys:   []string{"foo"},
			iter:            iterator.Once([]Row{{"foo": "bar"}}),
		},
		)
		assert.NoError(t, err)
		_, err = iterator.Collect(transformer)
		assert.ErrorContains(t, err, `failed to create Debezium payload: failed to convert row value for key "foo": test error`)
	}
	{
		// Happy path
		fieldConverters := []FieldConverter{
			{Name: "foo", ValueConverter: testConverter{intField: false}},
			{Name: "qux", ValueConverter: testConverter{intField: true}},
			{Name: "baz", ValueConverter: testConverter{intField: false}},
		}
		batches := [][]Row{{
			{"foo": "bar", "qux": 12, "baz": "corge"},
		}}
		transformer, err := NewDebeziumTransformer(mockAdatper{
			fieldConverters: fieldConverters,
			partitionKeys:   []string{"foo", "qux"},
			iter:            iterator.ForSlice(batches),
		},
		)
		assert.NoError(t, err)
		results, err := iterator.Collect(transformer)
		assert.NoError(t, err)
		assert.Len(t, results, 1)
		rows := results[0]
		assert.Len(t, rows, 1)
		rawMessage := rows[0]
		assert.Equal(t, Row{"foo": "bar", "qux": 12}, rawMessage.PartitionKey())
		assert.Equal(t, "im-a-little-topic-suffix", rawMessage.TopicSuffix())
		payload, isOk := rawMessage.Event().(*util.SchemaEventPayload)
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
					After:     map[string]any{"foo": "converted-bar", "qux": "converted-12", "baz": "converted-corge"},
					Source:    util.Source{Connector: "", TsMs: 12345, Database: "", Schema: "", Table: "im-a-little-table"},
					Operation: "r",
				},
			},
		)
		assert.Equal(t, expected, *payload)
	}
}

func TestDebeziumTransformer_CreatePayload(t *testing.T) {
	{
		// Field converter error
		fieldConverters := []FieldConverter{
			{Name: "qux", ValueConverter: testConverter{intField: true, returnErr: true}},
		}
		transformer, err := NewDebeziumTransformer(mockAdatper{fieldConverters: fieldConverters, iter: iterator.ForSlice([][]Row{})})
		assert.NoError(t, err)
		_, err = transformer.createPayload(Row{"qux": "quux"})
		assert.ErrorContains(t, err, `failed to convert row value for key "qux": test error`)
	}
	{
		// Happy path
		fieldConverters := []FieldConverter{
			{Name: "foo", ValueConverter: testConverter{intField: false}},
			{Name: "qux", ValueConverter: testConverter{intField: true}},
		}
		transformer, err := NewDebeziumTransformer(mockAdatper{fieldConverters: fieldConverters, iter: iterator.ForSlice([][]Row{})})
		assert.NoError(t, err)
		payload, err := transformer.createPayload(Row{"foo": "bar", "qux": "quux"})
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
					After:     map[string]any{"foo": "converted-bar", "qux": "converted-quux"},
					Source:    util.Source{TsMs: 12345, Table: "im-a-little-table"},
					Operation: "r",
				},
			},
		)
		assert.Equal(t, expected, payload)
	}
}

func TestDebeziumTransformer_PartitionKey(t *testing.T) {
	type _tc struct {
		name     string
		keys     []string
		row      Row
		expected map[string]any
	}

	testCases := []_tc{
		{
			name:     "no partition keys",
			row:      Row{},
			expected: map[string]any{},
		},
		{
			name:     "partition keys - empty row",
			keys:     []string{"foo", "bar"},
			row:      Row{},
			expected: map[string]any{"foo": nil, "bar": nil},
		},
		{
			name:     "partition keys - row has data",
			keys:     []string{"foo", "bar"},
			row:      Row{"foo": "a", "bar": 2, "baz": 3},
			expected: map[string]any{"foo": "a", "bar": 2},
		},
	}

	for _, testCase := range testCases {
		transformer, err := NewDebeziumTransformer(mockAdatper{partitionKeys: testCase.keys})
		assert.NoError(t, err)
		assert.Equal(t, testCase.expected, transformer.partitionKey(testCase.row), testCase.name)
	}
}

func TestConvertRow(t *testing.T) {
	{
		// Empty `valueConverters` + empty `row``
		value, err := convertRow(map[string]converters.ValueConverter{}, Row{})
		assert.NoError(t, err)
		assert.Equal(t, Row{}, value)
	}
	{
		// Empty `valueConverters` + non-empty `row``
		_, err := convertRow(map[string]converters.ValueConverter{}, Row{"foo": "bar"})
		assert.ErrorContains(t, err, `failed to get ValueConverter for key "foo"`)
	}
	{
		// Non-empty `valueConverters` + empty `row``
		value, err := convertRow(map[string]converters.ValueConverter{"foo": testConverter{}}, Row{})
		assert.NoError(t, err)
		assert.Equal(t, Row{}, value)
	}
	{
		// Non-empty `valueConverters` + non-empty `row`
		value, err := convertRow(
			map[string]converters.ValueConverter{"foo": testConverter{}, "baz": testConverter{}},
			Row{"foo": "bar", "baz": nil},
		)
		assert.NoError(t, err)
		assert.Equal(t, Row{"foo": "converted-bar", "baz": nil}, value)
	}
	{
		// Non-empty `valueConverters` + non-empty `row` + conversion error
		_, err := convertRow(
			map[string]converters.ValueConverter{"foo": testConverter{returnErr: true}, "baz": testConverter{}},
			Row{"foo": "bar", "baz": nil},
		)
		assert.ErrorContains(t, err, `failed to convert row value for key "foo": test error`)
	}
}
