package dynamo

import (
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func (d *DynamoDBTestSuite) Test_NewMessageFromExport() {
	type _tc struct {
		name          string
		item          dynamodb.ItemResponse
		keys          []string
		tableName     string
		expectedError string
	}

	tcs := []_tc{
		{
			name: "Test with empty item",
			item: dynamodb.ItemResponse{
				Item: map[string]*dynamodb.AttributeValue{},
			},
			keys:          []string{"id"},
			tableName:     "test",
			expectedError: "item is nil or keys do not exist in this item payload",
		},
		{
			name: "Test with empty keys",
			item: dynamodb.ItemResponse{
				Item: map[string]*dynamodb.AttributeValue{
					"id": {
						S: ptr.ToString("1"),
					},
				},
			},
			keys:          []string{},
			tableName:     "test",
			expectedError: "keys is nil",
		},
		{
			name: "Test with valid item and keys",
			item: dynamodb.ItemResponse{
				Item: map[string]*dynamodb.AttributeValue{
					"id": {
						S: ptr.ToString("1"),
					},
				},
			},
			keys:      []string{"id"},
			tableName: "test",
		},
	}

	for _, tc := range tcs {
		msg, err := NewMessageFromExport(tc.item, tc.keys, tc.tableName)
		if tc.expectedError != "" {
			assert.Equal(d.T(), tc.expectedError, err.Error(), tc.name)
		} else {
			assert.NoError(d.T(), err, tc.name)
			assert.Equal(d.T(), tc.tableName, msg.tableName, tc.name)
			assert.Equal(d.T(), "r", msg.op, tc.name)
		}
	}
}
