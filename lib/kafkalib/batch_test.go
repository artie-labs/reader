package kafkalib

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestBatch_IsValid(t *testing.T) {
	type _testCase struct {
		name        string
		msgs        []kafka.Message
		chunkSize   uint
		expectError bool
	}

	testCases := []_testCase{
		{
			name: "happy path",
			msgs: []kafka.Message{
				{Value: []byte("message1")},
				{Value: []byte("message2")},
			},
			chunkSize: 2,
		},
		{
			name: "happy path (chunkSize = 0)",
			msgs: []kafka.Message{
				{Value: []byte("message1")},
				{Value: []byte("message2")},
			},
			expectError: true,
		},
		{
			name:        "batch is empty",
			chunkSize:   2,
			expectError: true,
		},
	}

	for _, testCase := range testCases {
		b := NewBatch(testCase.msgs, testCase.chunkSize)
		actualErr := b.IsValid()
		if testCase.expectError {
			assert.Error(t, actualErr, testCase.name)
		} else {
			assert.NoError(t, actualErr, testCase.name)
		}
	}
}

func TestBatch_NextChunk(t *testing.T) {
	t.Run("NextChunk", func(t *testing.T) {
		messages := []kafka.Message{
			{Value: []byte("message1")},
			{Value: []byte("message2")},
			{Value: []byte("message3")},
		}
		batch := NewBatch(messages, 2)

		// First call to NextChunk
		chunk := batch.NextChunk()
		assert.Equal(t, 2, len(chunk), "Expected chunk size to be 2")
		assert.Equal(t, []byte("message1"), chunk[0].Value, "Expected first message in chunk to be message1")
		assert.Equal(t, []byte("message2"), chunk[1].Value, "Expected second message in chunk to be message2")

		// Second call to NextChunk
		chunk = batch.NextChunk()
		assert.Equal(t, 1, len(chunk), "Expected chunk size to be 1 for the remaining messages")
		assert.Equal(t, []byte("message3"), chunk[0].Value, "Expected the last message in chunk to be message3")

		// Third call to NextChunk should return an empty chunk as there are no more messages
		chunk = batch.NextChunk()
		assert.Empty(t, chunk, "Expected an empty chunk when there are no more messages")
	})
}
