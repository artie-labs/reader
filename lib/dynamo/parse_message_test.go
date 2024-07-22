package dynamo

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
)

func Test_NewMessage(t *testing.T) {
	{
		_, err := NewMessage(types.Record{}, "testTable")
		assert.ErrorContains(t, err, "record is nil or dynamodb does not exist in this event payload")

		// No keys.
		_, err = NewMessage(types.Record{Dynamodb: &types.StreamRecord{}}, "testTable")
		assert.ErrorContains(t, err, "keys is nil")
	}
	{
		// Insert
		msg, err := NewMessage(types.Record{
			Dynamodb: &types.StreamRecord{
				NewImage: map[string]types.AttributeValue{
					"foo": &types.AttributeValueMemberS{
						Value: "bar",
					},
				},
				Keys: map[string]types.AttributeValue{
					"user_id": &types.AttributeValueMemberS{
						Value: "123",
					},
				},
				ApproximateCreationDateTime: aws.Time(time.Date(2023, 8, 28, 0, 0, 0, 0, time.UTC)),
			},
			EventName: types.OperationTypeInsert,
		}, "testTable")

		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"foo": "bar"}, msg.afterRowData)
		assert.Equal(t, map[string]any{"user_id": "123"}, msg.primaryKey)
		assert.Equal(t, "c", msg.op)
		assert.Equal(t, "testTable", msg.tableName)
		assert.Equal(t, time.Date(2023, 8, 28, 0, 0, 0, 0, time.UTC), msg.executionTime)
	}
	{
		// Update
		msg, err := NewMessage(types.Record{
			Dynamodb: &types.StreamRecord{
				NewImage: map[string]types.AttributeValue{
					"foo": &types.AttributeValueMemberS{
						Value: "bar",
					},
				},
				Keys: map[string]types.AttributeValue{
					"user_id": &types.AttributeValueMemberS{
						Value: "123",
					},
				},
				ApproximateCreationDateTime: aws.Time(time.Date(2023, 8, 28, 0, 0, 0, 0, time.UTC)),
			},
			EventName: types.OperationTypeModify,
		}, "testTable")

		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"foo": "bar"}, msg.afterRowData)
		assert.Equal(t, map[string]any{"user_id": "123"}, msg.primaryKey)
		assert.Equal(t, "u", msg.op)
		assert.Equal(t, "testTable", msg.tableName)
		assert.Equal(t, time.Date(2023, 8, 28, 0, 0, 0, 0, time.UTC), msg.executionTime)
	}
	{
		// Delete
		msg, err := NewMessage(types.Record{
			Dynamodb: &types.StreamRecord{
				OldImage: map[string]types.AttributeValue{
					"foo": &types.AttributeValueMemberS{
						Value: "bar",
					},
				},
				Keys: map[string]types.AttributeValue{
					"user_id": &types.AttributeValueMemberS{
						Value: "123",
					},
				},
				ApproximateCreationDateTime: aws.Time(time.Date(2023, 8, 28, 0, 0, 0, 0, time.UTC)),
			},
			EventName: types.OperationTypeRemove,
		}, "testTable")

		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"foo": "bar"}, msg.beforeRowData)
		assert.Equal(t, map[string]any{"user_id": "123"}, msg.primaryKey)
		assert.Equal(t, 0, len(msg.afterRowData))
		assert.Equal(t, "d", msg.op)
		assert.Equal(t, "testTable", msg.tableName)
		assert.Equal(t, time.Date(2023, 8, 28, 0, 0, 0, 0, time.UTC), msg.executionTime)
	}
}

func Test_NewMessageFromExport(t *testing.T) {
	tcs := []struct {
		name        string
		item        map[string]ddbTypes.AttributeValue
		keys        []string
		tableName   string
		expectedErr string
	}{
		{
			name:        "Test with empty item",
			item:        map[string]ddbTypes.AttributeValue{},
			keys:        []string{"id"},
			tableName:   "test",
			expectedErr: "item is nil or keys do not exist in this item payload",
		},
		{
			name: "Test with empty keys",
			item: map[string]ddbTypes.AttributeValue{
				"id": &ddbTypes.AttributeValueMemberS{
					Value: "1",
				},
			},
			keys:        []string{},
			tableName:   "test",
			expectedErr: "keys is nil",
		},
		{
			name: "Test with valid item and keys",
			item: map[string]ddbTypes.AttributeValue{
				"id": &ddbTypes.AttributeValueMemberS{
					Value: "1",
				},
			},
			keys:      []string{"id"},
			tableName: "test",
		},
	}

	for _, tc := range tcs {
		msg, err := NewMessageFromExport(ddbTypes.ItemResponse{Item: tc.item}, tc.keys, tc.tableName)
		if tc.expectedErr != "" {
			assert.Equal(t, tc.expectedErr, err.Error(), tc.name)
		} else {
			assert.NoError(t, err, tc.name)
			assert.Equal(t, tc.tableName, msg.tableName, tc.name)
			assert.Equal(t, "r", msg.op, tc.name)
		}
	}
}
