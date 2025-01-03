package writers

import (
	"context"
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib/iterator"
	"github.com/artie-labs/reader/lib/kafkalib"
)

type mockDestination struct {
	messages  []kafkalib.Message
	emitError bool
}

func (m *mockDestination) CreateTable(_ context.Context, _ string, _ []columns.Column) error {
	return nil
}

func (m *mockDestination) Write(_ context.Context, msgs []kafkalib.Message) error {
	if m.emitError {
		return fmt.Errorf("test write-raw-messages error")
	}
	m.messages = append(m.messages, msgs...)
	return nil
}

func (m *mockDestination) OnComplete(_ context.Context) error {
	return nil
}

type errorIterator struct{}

func (m *errorIterator) HasNext() bool {
	return true
}

func (m *errorIterator) Next() ([]kafkalib.Message, error) {
	return nil, fmt.Errorf("test iteration error")
}

func TestWriter_Write(t *testing.T) {
	{
		// Empty iterator
		destination := &mockDestination{}
		writer := New(destination, false)
		iter := iterator.ForSlice([][]kafkalib.Message{})
		count, err := writer.Write(context.Background(), iter)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
		assert.Empty(t, destination.messages)
	}
	{
		// Iteration error
		destination := &mockDestination{}
		writer := New(destination, false)
		iter := &errorIterator{}
		_, err := writer.Write(context.Background(), iter)
		assert.ErrorContains(t, err, "failed to iterate over messages: test iteration error")
		assert.Empty(t, destination.messages)
	}
	{
		// Two empty batches
		destination := &mockDestination{}
		writer := New(destination, false)
		iter := iterator.ForSlice([][]kafkalib.Message{{}, {}})
		count, err := writer.Write(context.Background(), iter)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
		assert.Empty(t, destination.messages)
	}
	{
		// Three batches, two non-empty
		destination := &mockDestination{}
		writer := New(destination, false)
		iter := iterator.ForSlice([][]kafkalib.Message{
			{kafkalib.NewMessage("a", debezium.FieldsObject{}, nil, nil)},
			{},
			{
				kafkalib.NewMessage("b", debezium.FieldsObject{}, nil, nil),
				kafkalib.NewMessage("c", debezium.FieldsObject{}, nil, nil),
			},
		})
		count, err := writer.Write(context.Background(), iter)
		assert.NoError(t, err)
		assert.Equal(t, 3, count)
		assert.Len(t, destination.messages, 3)
		assert.Equal(t, destination.messages[0].Topic(""), "a")
		assert.Equal(t, destination.messages[1].Topic(""), "b")
		assert.Equal(t, destination.messages[2].Topic(""), "c")
	}
	{
		// Destination error
		destination := &mockDestination{emitError: true}
		writer := New(destination, false)
		iter := iterator.Once([]kafkalib.Message{kafkalib.NewMessage("a", debezium.FieldsObject{}, nil, nil)})
		_, err := writer.Write(context.Background(), iter)
		assert.ErrorContains(t, err, "failed to write messages: test write-raw-messages error")
		assert.Empty(t, destination.messages)
	}
}
