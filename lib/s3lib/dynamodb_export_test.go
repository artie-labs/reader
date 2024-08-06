package s3lib

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAssertType(t *testing.T) {
	{
		// String to string
		val, err := assertType[string]("hello")
		assert.NoError(t, err)
		assert.Equal(t, "hello", val)
	}
	{
		// Int to string
		_, err := assertType[string](1)
		assert.ErrorContains(t, err, "expected type string, got int")
	}
	{
		// Boolean to boolean
		val, err := assertType[bool](true)
		assert.NoError(t, err)
		assert.Equal(t, true, val)
	}
	{
		// String to boolean
		_, err := assertType[bool]("true")
		assert.ErrorContains(t, err, "expected type bool, got string")
	}
}

const validDynamoDBJSON = `{
    "Item": {
        "account_id": {
            "S": "account-9825"
        },
        "user_id": {
            "S": "user_id_1"
        },
        "is_null": {
            "NULL": true
        },
        "sample_list": {
            "L": [
                {
                    "S": "item1"
                },
                {
                    "N": "2"
                }
            ]
        },
        "flag": {
            "BOOL": false
        },
        "string_set": {
            "SS": [
                "value2",
                "value44",
                "value55",
                "value66",
                "value1"
            ]
        },
        "random_number": {
            "N": "4851973137566368817"
        },
        "number_set": {
            "NS": [
                "3",
                "2",
                "1"
            ]
        },
        "sample_map": {
            "M": {
                "key1": {
                    "S": "value1"
                },
                "key2": {
                    "N": "2"
                }
            }
        }
    }
}`

func TestParseDynamoDBJSON(t *testing.T) {
	{
		// Valid JSON
		value, err := parseDynamoDBJSON([]byte(validDynamoDBJSON))
		assert.NoError(t, err)
		assert.Equal(t, 9, len(value))

		// Check keys
		value, isOk := value["account_id"].(*types.AttributeValueMemberS{})
		assert.True(t, isOk)
	}
}
