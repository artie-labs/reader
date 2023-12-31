package offsets

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"time"
)

func ptrDuration(d time.Duration) *time.Duration {
	return &d
}

func (o *OffsetsTestSuite) TestOffsets_Complete() {
	offsetsFilePath := "/tmp/offsets-test"
	assert.NoError(o.T(), os.RemoveAll(offsetsFilePath)) // Delete if prev run wasn't clean.

	defer assert.NoError(o.T(), os.RemoveAll(offsetsFilePath)) // Delete the file we do create during the test.

	storage := NewStorage(o.ctx, offsetsFilePath, ptrDuration(50*time.Millisecond), ptrDuration(50*time.Millisecond))
	processedShards := []string{"foo", "bar", "xyz"}

	// It should all return `False` because the file doesn't exist and we didn't load anything yet.
	for _, processedShard := range processedShards {
		assert.False(o.T(), storage.GetShardProcessed(processedShard), processedShard)
		storage.SetShardProcessed(processedShard)
	}

	shardToSequenceNumber := map[string]string{
		"shard-1": "123",
		"shard-2": "456",
		"shard-3": "789",
	}

	for shard, sequenceNumber := range shardToSequenceNumber {
		_, isOk := storage.LastProcessedSequenceNumber(shard)
		assert.False(o.T(), isOk, shard)

		storage.SetLastProcessedSequenceNumber(shard, sequenceNumber)
	}

	// Sleep, wait for the file to be committed to disk and then reload the storage.
	time.Sleep(75 * time.Millisecond) // Wait for the file to be written.
	storage = NewStorage(o.ctx, offsetsFilePath, ptrDuration(50*time.Millisecond), ptrDuration(50*time.Millisecond))
	for _, processedShard := range processedShards {
		assert.True(o.T(), storage.GetShardProcessed(processedShard),
			fmt.Sprintf("shard: %s, value: %v", processedShard, storage.GetShardProcessed(processedShard)))
	}

	for shard, sequenceNumber := range shardToSequenceNumber {
		retrievedSeqNumber, isOk := storage.LastProcessedSequenceNumber(shard)
		assert.True(o.T(), isOk, shard)
		assert.Equal(o.T(), sequenceNumber, retrievedSeqNumber, shard)
	}
}
