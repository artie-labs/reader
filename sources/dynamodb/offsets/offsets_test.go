package offsets

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func ptrDuration(d time.Duration) *time.Duration {
	return &d
}

func TestOffsets_Complete(t *testing.T) {
	offsetsFilePath := filepath.Join(t.TempDir(), "offsets-test")

	storage := NewStorage(offsetsFilePath, ptrDuration(50*time.Millisecond), ptrDuration(50*time.Millisecond))
	processedShards := []string{"foo", "bar", "xyz"}

	// It should all return `False` because the file doesn't exist and we didn't load anything yet.
	for _, processedShard := range processedShards {
		assert.False(t, storage.GetShardProcessed(processedShard), processedShard)
		storage.SetShardProcessed(processedShard)
	}

	shardToSequenceNumber := map[string]string{
		"shard-1": "123",
		"shard-2": "456",
		"shard-3": "789",
	}

	for shard, sequenceNumber := range shardToSequenceNumber {
		_, isOk := storage.LastProcessedSequenceNumber(shard)
		assert.False(t, isOk, shard)

		storage.SetLastProcessedSequenceNumber(shard, sequenceNumber)
	}

	// Sleep, wait for the file to be committed to disk and then reload the storage.
	time.Sleep(75 * time.Millisecond) // Wait for the file to be written.
	storage = NewStorage(offsetsFilePath, ptrDuration(50*time.Millisecond), ptrDuration(50*time.Millisecond))
	for _, processedShard := range processedShards {
		assert.True(t, storage.GetShardProcessed(processedShard),
			fmt.Sprintf("shard: %s, value: %v", processedShard, storage.GetShardProcessed(processedShard)))
	}

	for shard, sequenceNumber := range shardToSequenceNumber {
		retrievedSeqNumber, isOk := storage.LastProcessedSequenceNumber(shard)
		assert.True(t, isOk, shard)
		assert.Equal(t, sequenceNumber, retrievedSeqNumber, shard)
	}
}
