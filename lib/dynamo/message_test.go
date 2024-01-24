package dynamo

import (
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/stretchr/testify/assert"
)

func TestNewMessage(t *testing.T) {
	type _tc struct {
		name      string
		record    *dynamodbstreams.Record
		tableName string

		expectErr       bool
		expectedMessage *Message
	}

	tcs := []_tc{
		{
			name:            "nil record",
			record:          nil,
			tableName:       "testTable",
			expectErr:       true,
			expectedMessage: nil,
		},
		{
			name:            "nil dynamodb",
			record:          &dynamodbstreams.Record{},
			tableName:       "testTable",
			expectErr:       true,
			expectedMessage: nil,
		},
		{
			name: "empty keys",
			record: &dynamodbstreams.Record{
				Dynamodb: &dynamodbstreams.StreamRecord{},
			},
			tableName:       "testTable",
			expectErr:       true,
			expectedMessage: nil,
		},
		{
			name: "EventName INSERT",
			record: &dynamodbstreams.Record{
				Dynamodb: &dynamodbstreams.StreamRecord{
					Keys: map[string]*dynamodb.AttributeValue{
						"user_id": {
							S: ptr.ToString("123"),
						},
					},
					ApproximateCreationDateTime: aws.Time(time.Date(2023, 8, 28, 0, 0, 0, 0, time.UTC)),
				},
				EventName: ptr.ToString("INSERT"),
			},
			tableName: "testTable",
			expectErr: false,
			expectedMessage: &Message{
				op:            "c",
				tableName:     "testTable",
				executionTime: time.Date(2023, 8, 28, 0, 0, 0, 0, time.UTC),
				rowData:       map[string]interface{}{},
				primaryKey: map[string]interface{}{
					"user_id": "123",
				},
			},
		},
		{
			name: "EventName MODIFY",
			record: &dynamodbstreams.Record{
				Dynamodb: &dynamodbstreams.StreamRecord{
					Keys: map[string]*dynamodb.AttributeValue{
						"user_id": {
							S: ptr.ToString("123"),
						},
					},
					ApproximateCreationDateTime: aws.Time(time.Date(2023, 8, 28, 0, 0, 0, 0, time.UTC)),
				},
				EventName: ptr.ToString("MODIFY"),
			},
			tableName: "testTable",
			expectErr: false,
			expectedMessage: &Message{
				op:            "u",
				executionTime: time.Date(2023, 8, 28, 0, 0, 0, 0, time.UTC),
				tableName:     "testTable",
				rowData:       map[string]interface{}{},
				primaryKey: map[string]interface{}{
					"user_id": "123",
				},
			},
		},
		{
			name: "EventName REMOVE",
			record: &dynamodbstreams.Record{
				Dynamodb: &dynamodbstreams.StreamRecord{
					Keys: map[string]*dynamodb.AttributeValue{
						"user_id": {
							S: ptr.ToString("123"),
						},
					},
					ApproximateCreationDateTime: aws.Time(time.Date(2023, 8, 28, 0, 0, 0, 0, time.UTC)),
				},
				EventName: aws.String("REMOVE"),
			},
			tableName: "testTable",
			expectErr: false,
			expectedMessage: &Message{
				op:            "d",
				tableName:     "testTable",
				executionTime: time.Date(2023, 8, 28, 0, 0, 0, 0, time.UTC),
				rowData:       map[string]interface{}{},
				primaryKey: map[string]interface{}{
					"user_id": "123",
				},
			},
		},
		{
			name: "With ApproximateCreationDateTime",
			record: &dynamodbstreams.Record{
				Dynamodb: &dynamodbstreams.StreamRecord{
					Keys: map[string]*dynamodb.AttributeValue{"key": {
						S: ptr.ToString("value"),
					}},
					ApproximateCreationDateTime: aws.Time(time.Date(2023, 8, 28, 0, 0, 0, 0, time.UTC)),
				},
				EventName: aws.String("INSERT"),
			},
			tableName: "testTable",
			expectErr: false,
			expectedMessage: &Message{
				op:            "c",
				tableName:     "testTable",
				executionTime: time.Date(2023, 8, 28, 0, 0, 0, 0, time.UTC),
				rowData:       map[string]interface{}{},
				primaryKey:    map[string]interface{}{"key": "value"},
			},
		},
	}

	for _, tc := range tcs {
		actualMessage, actualErr := NewMessage(tc.record, tc.tableName)
		if tc.expectErr {
			assert.Error(t, actualErr, tc.name)
		} else {
			assert.NoError(t, actualErr, tc.name)
			assert.Equal(t, tc.expectedMessage, actualMessage, tc.name)
		}
	}
}

func TestTransformAttributeValue(t *testing.T) {
	type _tc struct {
		name          string
		attr          *dynamodb.AttributeValue
		expectedValue interface{}
	}

	tcs := []_tc{
		{
			name: "string",
			attr: &dynamodb.AttributeValue{
				S: ptr.ToString("hello"),
			},
			expectedValue: "hello",
		},
		{
			name: "number",
			attr: &dynamodb.AttributeValue{
				N: ptr.ToString("123"),
			},
			expectedValue: float64(123),
		},
		{
			name: "boolean",
			attr: &dynamodb.AttributeValue{
				BOOL: ptr.ToBool(true),
			},
			expectedValue: true,
		},
		{
			name: "map",
			attr: &dynamodb.AttributeValue{
				M: map[string]*dynamodb.AttributeValue{
					"foo": {
						S: ptr.ToString("bar"),
					},
					"bar": {
						N: ptr.ToString("123"),
					},
					"nested_map": {
						M: map[string]*dynamodb.AttributeValue{
							"foo": {
								S: ptr.ToString("bar"),
							},
						},
					},
				},
			},
			expectedValue: map[string]interface{}{
				"foo": "bar",
				"bar": float64(123),
				"nested_map": map[string]interface{}{
					"foo": "bar",
				},
			},
		},
		{
			name: "list",
			attr: &dynamodb.AttributeValue{
				L: []*dynamodb.AttributeValue{
					{
						S: ptr.ToString("foo"),
					},
					{
						N: ptr.ToString("123"),
					},
					{
						M: map[string]*dynamodb.AttributeValue{
							"foo": {
								S: ptr.ToString("bar"),
							},
						},
					},
				},
			},
			expectedValue: []interface{}{
				"foo",
				float64(123),
				map[string]interface{}{
					"foo": "bar",
				},
			},
		},
		{
			name: "string set",
			attr: &dynamodb.AttributeValue{
				SS: []*string{
					ptr.ToString("foo"),
					ptr.ToString("bar"),
				},
			},
			expectedValue: []string{
				"foo",
				"bar",
			},
		},
		{
			name: "number set",
			attr: &dynamodb.AttributeValue{
				NS: []*string{
					ptr.ToString("123"),
					ptr.ToString("456"),
				},
			},
			expectedValue: []float64{
				123,
				456,
			},
		},
	}

	for _, tc := range tcs {
		actualValue := transformAttributeValue(tc.attr)
		assert.Equal(t, tc.expectedValue, actualValue, tc.name)
	}
}
