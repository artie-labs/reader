package writers

import (
	"context"
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/iterator"
)

type mockDestination struct {
	messages  []lib.RawMessage
	emitError bool
}

func (m *mockDestination) CreateTable(_ context.Context, _ sql.TableIdentifier, _ []columns.Column) error {
	return nil
}

func (m *mockDestination) Write(_ context.Context, msgs []lib.RawMessage) error {
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

func (m *errorIterator) Next() ([]lib.RawMessage, error) {
	return nil, fmt.Errorf("test iteration error")
}

func TestWriter_Write(t *testing.T) {
	{
		// Empty iterator
		destination := &mockDestination{}
		writer := New(destination, false)
		iter := iterator.ForSlice([][]lib.RawMessage{})
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
		iter := iterator.ForSlice([][]lib.RawMessage{{}, {}})
		count, err := writer.Write(context.Background(), iter)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
		assert.Empty(t, destination.messages)
	}
	{
		// Three batches, two non-empty
		destination := &mockDestination{}
		writer := New(destination, false)
		iter := iterator.ForSlice([][]lib.RawMessage{
			{lib.NewRawMessage("a", nil, nil)},
			{},
			{
				lib.NewRawMessage("b", nil, nil),
				lib.NewRawMessage("c", nil, nil),
			},
		})
		count, err := writer.Write(context.Background(), iter)
		assert.NoError(t, err)
		assert.Equal(t, 3, count)
		assert.Len(t, destination.messages, 3)
		assert.Equal(t, destination.messages[0].TopicSuffix(), "a")
		assert.Equal(t, destination.messages[1].TopicSuffix(), "b")
		assert.Equal(t, destination.messages[2].TopicSuffix(), "c")
	}
	{
		// Destination error
		destination := &mockDestination{emitError: true}
		writer := New(destination, false)
		iter := iterator.Once([]lib.RawMessage{lib.NewRawMessage("a", nil, nil)})
		_, err := writer.Write(context.Background(), iter)
		assert.ErrorContains(t, err, "failed to write messages: test write-raw-messages error")
		assert.Empty(t, destination.messages)
	}
}
