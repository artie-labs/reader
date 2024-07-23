package dynamo

import (
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTransformAttributeValue(t *testing.T) {
	{
		// String
		actualValue, fieldType, err := transformAttributeValue(&dynamodb.AttributeValue{
			S: ptr.ToString("hello"),
		})
		assert.NoError(t, err, err)
		assert.Equal(t, "hello", actualValue)
		assert.Equal(t, debezium.String, fieldType)
	}
	{
		// Number
		actualValue, fieldType, err := transformAttributeValue(&dynamodb.AttributeValue{
			N: ptr.ToString("123"),
		})
		assert.NoError(t, err, err)
		assert.Equal(t, float64(123), actualValue)
		assert.Equal(t, debezium.Float, fieldType)
	}
	{
		// Boolean
		actualValue, fieldType, err := transformAttributeValue(&dynamodb.AttributeValue{
			BOOL: ptr.ToBool(true),
		})
		assert.NoError(t, err, err)
		assert.Equal(t, true, actualValue)
		assert.Equal(t, debezium.Boolean, fieldType)
	}
	{
		// Map
		actualValue, fieldType, err := transformAttributeValue(&dynamodb.AttributeValue{
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
		actualValue, fieldType, err := transformAttributeValue(&dynamodb.AttributeValue{
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
		actualValue, fieldType, err := transformAttributeValue(&dynamodb.AttributeValue{
			SS: []*string{
				ptr.ToString("foo"),
				ptr.ToString("bar"),
			},
		})

		assert.NoError(t, err, err)
		assert.Equal(t, []string{"foo", "bar"}, actualValue)
		assert.Equal(t, debezium.Array, fieldType)
	}
	{
		// Number set
		actualValue, fieldType, err := transformAttributeValue(&dynamodb.AttributeValue{
			NS: []*string{
				ptr.ToString("123"),
				ptr.ToString("456"),
			},
		})

		assert.NoError(t, err, err)
		assert.Equal(t, []float64{123, 456}, actualValue)
		assert.Equal(t, debezium.Array, fieldType)
	}
}
