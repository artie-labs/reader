package dynamo

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func RetrievePrimaryKeys(ctx context.Context, client *dynamodb.Client, tableName string) ([]string, error) {
	output, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &tableName})
	if err != nil {
		return nil, err
	}

	var keys []string
	for _, key := range output.Table.KeySchema {
		if key.AttributeName != nil {
			keys = append(keys, *key.AttributeName)
		} else {
			// Should not be possible, attributeName is required.
			return nil, fmt.Errorf("key %v does not have attribute name", key)
		}
	}

	return keys, nil
}
