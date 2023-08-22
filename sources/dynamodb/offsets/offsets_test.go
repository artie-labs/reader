package offsets

import (
	"github.com/stretchr/testify/assert"
	"os"
)

func (o *OffsetsTestSuite) TestOffsets_Complete() {
	offsetsFilePath := "/tmp/offsets-test"
	err := os.RemoveAll(offsetsFilePath)
	assert.NoError(o.T(), err)

	storage := NewStorage(o.ctx, offsetsFilePath)
	originalLastProcessedSeqNumbers := map[string]string{
		"shard-1": "123",
		"shard-2": "456",
		"shard-3": "789",
	}

	// Try to save a bunch of times, file will not exist since shouldSave = false
	_, err = os.Open(offsetsFilePath)
	assert.Error(o.T(), err)

	for shard, lastProcessedSequenceNumber := range originalLastProcessedSeqNumbers {
		storage.SetLastProcessedSequenceNumber(shard, lastProcessedSequenceNumber)
	}

	storage.Save(o.ctx)
	storage.lastProcessedSeqNumbers = map[string]string{}
	storage.load(o.ctx)

	assert.False(o.T(), storage.shouldSave)
	assert.Equal(o.T(), originalLastProcessedSeqNumbers, storage.lastProcessedSeqNumbers)
}
