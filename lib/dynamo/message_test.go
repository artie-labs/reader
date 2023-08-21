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
		},
	}

	for _, tc := range tcs {
		actualValue := transformAttributeValue(tc.attr)
		assert.Equal(d.T(), tc.expectedValue, actualValue, tc.name)
	}
}
