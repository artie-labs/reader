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
        "b": {
            "B": "aGVsbG8gd29ybGQ="
        },
        "bs": {
            "BS": [
                "aGVsbG8=",
                "d29ybGQ="
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
		assert.Equal(t, 11, len(value))

		{
			// String
			val, isOk := value["account_id"].(*types.AttributeValueMemberS)
			assert.True(t, isOk)
			assert.Equal(t, "account-9825", val.Value)
		}
		{
			// Number
			val, isOk := value["random_number"].(*types.AttributeValueMemberN)
			assert.True(t, isOk)
			assert.Equal(t, "4851973137566368817", val.Value)
		}
		{
			// Boolean
			val, isOk := value["flag"].(*types.AttributeValueMemberBOOL)
			assert.True(t, isOk)
			assert.Equal(t, false, val.Value)
		}
		{
			// List
			val, isOk := value["sample_list"].(*types.AttributeValueMemberL)
			assert.True(t, isOk)
			assert.Equal(t, 2, len(val.Value))

			assert.Equal(t, "item1", val.Value[0].(*types.AttributeValueMemberS).Value)
			assert.Equal(t, "2", val.Value[1].(*types.AttributeValueMemberN).Value)
		}
		{
			// String set
			val, isOk := value["string_set"].(*types.AttributeValueMemberSS)
			assert.True(t, isOk)
			assert.Equal(t, 5, len(val.Value))
			assert.Equal(t, []string{"value2", "value44", "value55", "value66", "value1"}, val.Value)
		}
		{
			// Bytes
			val, isOk := value["b"].(*types.AttributeValueMemberB)
			assert.True(t, isOk)
			assert.Equal(t, []byte("hello world"), val.Value)
		}
		{
			// Bytes set
			val, isOk := value["bs"].(*types.AttributeValueMemberBS)
			assert.True(t, isOk)
			assert.Equal(t, 2, len(val.Value))
			assert.Equal(t, []byte("hello"), val.Value[0])
			assert.Equal(t, []byte("world"), val.Value[1])
		}
		{
			// Number set
			val, isOk := value["number_set"].(*types.AttributeValueMemberNS)
			assert.True(t, isOk)
			assert.Equal(t, 3, len(val.Value))
			assert.Equal(t, []string{"3", "2", "1"}, val.Value)
		}
		{
			// Map
			val, isOk := value["sample_map"].(*types.AttributeValueMemberM)
			assert.True(t, isOk)
			assert.Equal(t, 2, len(val.Value))

			assert.Equal(t, "value1", val.Value["key1"].(*types.AttributeValueMemberS).Value)
			assert.Equal(t, "2", val.Value["key2"].(*types.AttributeValueMemberN).Value)
		}
	}
}
