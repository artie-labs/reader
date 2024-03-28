package writer

import (
	"context"
	"fmt"
	"testing"

	"github.com/artie-labs/reader/lib"
	"github.com/stretchr/testify/assert"
)

type mockDestination struct {
	messages  []lib.RawMessage
	emitError bool
}

func (m *mockDestination) WriteRawMessages(ctx context.Context, msgs []lib.RawMessage) error {
	if m.emitError {
		return fmt.Errorf("test write raw messages error")
	}
	m.messages = append(m.messages, msgs...)
	return nil
}

type mockIterator struct {
	emitError bool
	index     int
	batches   [][]lib.RawMessage
}

func (m *mockIterator) HasNext() bool {
	return m.index < len(m.batches)
}

func (m *mockIterator) Next() ([]lib.RawMessage, error) {
	if m.emitError {
		return nil, fmt.Errorf("test iteration error")
	}

	if !m.HasNext() {
		return nil, fmt.Errorf("done")
	}
	result := m.batches[m.index]
	m.index++
	return result, nil
}

func TestWriter_Write(t *testing.T) {
	{
		// Empty iterator
		destination := &mockDestination{}
		writer := New(destination)
		iterator := &mockIterator{}
		count, err := writer.Write(context.Background(), iterator)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
		assert.Empty(t, destination.messages)
	}
	{
		// Iteration error
		destination := &mockDestination{}
		writer := New(destination)
		iterator := &mockIterator{emitError: true, batches: [][]lib.RawMessage{{{TopicSuffix: "a"}}}}
		_, err := writer.Write(context.Background(), iterator)
		assert.ErrorContains(t, err, "failed to iterate over messages: test iteration error")
		assert.Empty(t, destination.messages)
	}
	{
		// Two empty batches
		destination := &mockDestination{}
		writer := New(destination)
		iterator := &mockIterator{batches: [][]lib.RawMessage{{}, {}}}
		count, err := writer.Write(context.Background(), iterator)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
		assert.Empty(t, destination.messages)
	}
	{
		// Three batches, two non-empty
		destination := &mockDestination{}
		writer := New(destination)
		iterator := &mockIterator{batches: [][]lib.RawMessage{{{TopicSuffix: "a"}}, {}, {{TopicSuffix: "b"}, {TopicSuffix: "c"}}}}
		count, err := writer.Write(context.Background(), iterator)
		assert.NoError(t, err)
		assert.Equal(t, 3, count)
		assert.Len(t, destination.messages, 3)
	}
	{
		// Destination error
		destination := &mockDestination{emitError: true}
		writer := New(destination)
		iterator := &mockIterator{batches: [][]lib.RawMessage{{{TopicSuffix: "a"}}}}
		_, err := writer.Write(context.Background(), iterator)
		assert.ErrorContains(t, err, "failed to write messages: test write raw messages error")
		assert.Empty(t, destination.messages)
	}
}
