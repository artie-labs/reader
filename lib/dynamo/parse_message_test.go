package dynamo

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func Test_NewMessage(t *testing.T) {
	{
		// Invalid payloads
		_, err := NewMessage(nil, "testTable")
		assert.ErrorContains(t, err, "record is nil or dynamodb does not exist in this event payload")

		_, err = NewMessage(&dynamodbstreams.Record{}, "testTable")
		assert.ErrorContains(t, err, "record is nil or dynamodb does not exist in this event payload")

		// No keys.
		_, err = NewMessage(&dynamodbstreams.Record{Dynamodb: &dynamodbstreams.StreamRecord{}}, "testTable")
		assert.ErrorContains(t, err, "keys is nil")
	}
	{
		// Insert
		msg, err := NewMessage(&dynamodbstreams.Record{
			Dynamodb: &dynamodbstreams.StreamRecord{
				NewImage: map[string]*dynamodb.AttributeValue{
					"foo": {
						S: ptr.ToString("bar"),
					},
				},
				Keys: map[string]*dynamodb.AttributeValue{
					"user_id": {
						S: ptr.ToString("123"),
					},
				},
				ApproximateCreationDateTime: aws.Time(time.Date(2023, 8, 28, 0, 0, 0, 0, time.UTC)),
			},
			EventName: ptr.ToString("INSERT"),
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
		msg, err := NewMessage(&dynamodbstreams.Record{
			Dynamodb: &dynamodbstreams.StreamRecord{
				NewImage: map[string]*dynamodb.AttributeValue{
					"foo": {
						S: ptr.ToString("bar"),
					},
				},
				Keys: map[string]*dynamodb.AttributeValue{
					"user_id": {
						S: ptr.ToString("123"),
					},
				},
				ApproximateCreationDateTime: aws.Time(time.Date(2023, 8, 28, 0, 0, 0, 0, time.UTC)),
			},
			EventName: ptr.ToString("MODIFY"),
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
		msg, err := NewMessage(&dynamodbstreams.Record{
			Dynamodb: &dynamodbstreams.StreamRecord{
				OldImage: map[string]*dynamodb.AttributeValue{
					"foo": {
						S: ptr.ToString("bar"),
					},
				},
				Keys: map[string]*dynamodb.AttributeValue{
					"user_id": {
						S: ptr.ToString("123"),
					},
				},
				ApproximateCreationDateTime: aws.Time(time.Date(2023, 8, 28, 0, 0, 0, 0, time.UTC)),
			},
			EventName: ptr.ToString("REMOVE"),
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
		item        dynamodb.ItemResponse
		keys        []string
		tableName   string
		expectedErr string
	}{
		{
			name: "Test with empty item",
			item: dynamodb.ItemResponse{
				Item: map[string]*dynamodb.AttributeValue{},
			},
			keys:        []string{"id"},
			tableName:   "test",
			expectedErr: "item is nil or keys do not exist in this item payload",
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
			keys:        []string{},
			tableName:   "test",
			expectedErr: "keys is nil",
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
		if tc.expectedErr != "" {
			assert.Equal(t, tc.expectedErr, err.Error(), tc.name)
		} else {
			assert.NoError(t, err, tc.name)
			assert.Equal(t, tc.tableName, msg.tableName, tc.name)
			assert.Equal(t, "r", msg.op, tc.name)
		}
	}
}
