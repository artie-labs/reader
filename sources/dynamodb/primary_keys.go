package dynamodb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// retrievePrimaryKeys - This function is called when we process the DynamoDB table snapshot.
// This is because the snapshot is a JSON file and it does not contain which are the partition and sort keys.
func (s *SnapshotStore) retrievePrimaryKeys(ctx context.Context) ([]string, error) {
	output, err := s.dynamoDBClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &s.tableName,
	})

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
