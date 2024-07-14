package dynamo

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
)

func TestTransformAttributeValue(t *testing.T) {
	type _tc struct {
		name          string
		attr          types.AttributeValue
		expectedValue interface{}
	}

	tcs := []_tc{
		{
			name: "string",
			attr: &types.AttributeValueMemberS{
				Value: "hello",
			},
			expectedValue: "hello",
		},
		{
			name: "number",
			attr: &types.AttributeValueMemberN{
				Value: "123",
			},
			expectedValue: float64(123),
		},
		{
			name: "boolean",
			attr: &types.AttributeValueMemberBOOL{
				Value: true,
			},
			expectedValue: true,
		},
		{
			name: "map",
			attr: &types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"foo": &types.AttributeValueMemberS{
						Value: "bar",
					},
					"bar": &types.AttributeValueMemberN{
						Value: "123",
					},
					"nested_map": &types.AttributeValueMemberM{
						Value: map[string]types.AttributeValue{
							"foo": &types.AttributeValueMemberS{
								Value: "bar",
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
			attr: &types.AttributeValueMemberL{
				Value: []types.AttributeValue{
					&types.AttributeValueMemberS{Value: "foo"},
					&types.AttributeValueMemberN{Value: "123"},
					&types.AttributeValueMemberM{
						Value: map[string]types.AttributeValue{
							"foo": &types.AttributeValueMemberS{Value: "bar"},
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
			attr: &types.AttributeValueMemberSS{
				Value: []string{"foo", "bar"},
			},
			expectedValue: []string{"foo", "bar"},
		},
		{
			name: "number set",
			attr: &types.AttributeValueMemberNS{
				Value: []string{"123", "456"},
			},
			expectedValue: []float64{123, 456},
		},
	}

	for _, tc := range tcs {
		actualValue, err := transformAttributeValue(tc.attr)
		assert.NoError(t, err, tc.name)
		assert.Equal(t, tc.expectedValue, actualValue, tc.name)
	}
}
