package dynamo

import (
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func (d *DynamoDBTestSuite) TestTransformAttributeValue() {
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
		assert.Equal(d.T(), tc.expectedValue, actualValue, tc.name)
	}
}
