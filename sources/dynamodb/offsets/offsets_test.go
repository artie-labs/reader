package offsets

import (
	"github.com/artie-labs/reader/sources/dynamodb"
	"github.com/stretchr/testify/assert"
	"os"
)

func (d *dynamodb.DynamoDBTestSuite) TestOffsets_Complete() {
	offsetsFilePath := "/tmp/offsets-test"
	err := os.RemoveAll(offsetsFilePath)
	assert.NoError(d.T(), err)

	s := dynamodb.Store{
		offsetFilePath: "/tmp/offsets-test",
	}

	originalLastProcessedSeqNumbers := map[string]string{
		"shard-1": "123",
		"shard-2": "456",
		"shard-3": "789",
	}

	s.lastProcessedSeqNumbers = originalLastProcessedSeqNumbers
	s.saveOffsets(d.ctx)

	s.lastProcessedSeqNumbers = map[string]string{}
	s.loadOffsets(d.ctx)

	assert.Equal(d.T(), originalLastProcessedSeqNumbers, s.lastProcessedSeqNumbers)
}
