package dynamo

import (
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTransformAttributeValue(t *testing.T) {
	{
		// String
		actualValue, fieldType, err := transformAttributeValue(&types.AttributeValueMemberS{
			Value: "hello",
		})

		assert.NoError(t, err, err)
		assert.Equal(t, "hello", actualValue)
		assert.Equal(t, debezium.String, fieldType)
	}
	{
		// Number
		actualValue, fieldType, err := transformAttributeValue(&types.AttributeValueMemberN{
			Value: "123",
		})
		assert.NoError(t, err, err)
		assert.Equal(t, float64(123), actualValue)
		assert.Equal(t, debezium.Float, fieldType)
	}
	{
		// Boolean
		actualValue, fieldType, err := transformAttributeValue(&types.AttributeValueMemberBOOL{
			Value: true,
		})
		assert.NoError(t, err, err)
		assert.Equal(t, true, actualValue)
		assert.Equal(t, debezium.Boolean, fieldType)
	}
	{
		// Map
		actualValue, fieldType, err := transformAttributeValue(&types.AttributeValueMemberM{
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
		})

		assert.NoError(t, err, err)
		assert.Equal(t, map[string]any{
			"foo": "bar",
			"bar": float64(123),
			"nested_map": map[string]any{
				"foo": "bar",
			},
		}, actualValue)
		assert.Equal(t, debezium.Map, fieldType)
	}
	{
		// List
		actualValue, fieldType, err := transformAttributeValue(&types.AttributeValueMemberL{
			Value: []types.AttributeValue{
				&types.AttributeValueMemberS{
					Value: "foo",
				},
				&types.AttributeValueMemberN{
					Value: "123",
				},
				&types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"foo": &types.AttributeValueMemberS{
							Value: "bar",
						},
					},
				},
			},
		})

		assert.NoError(t, err, err)
		assert.Equal(t, []any{
			"foo",
			float64(123),
			map[string]any{
				"foo": "bar",
			},
		}, actualValue)
		assert.Equal(t, debezium.Array, fieldType)
	}
	{
		// String set
		actualValue, fieldType, err := transformAttributeValue(&types.AttributeValueMemberSS{
			Value: []string{"foo", "bar"},
		})

		assert.NoError(t, err, err)
		assert.Equal(t, []string{"foo", "bar"}, actualValue)
		assert.Equal(t, debezium.Array, fieldType)
	}
	{
		// Number set
		actualValue, fieldType, err := transformAttributeValue(&types.AttributeValueMemberNS{
			Value: []string{"123", "456"},
		})

		assert.NoError(t, err, err)
		assert.Equal(t, []float64{123, 456}, actualValue)
		assert.Equal(t, debezium.Array, fieldType)
	}
}
