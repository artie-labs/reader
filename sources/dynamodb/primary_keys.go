package dynamodb

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// retrievePrimaryKeys - This function is called when we process the DynamoDB table snapshot.
// This is because the snapshot is a JSON file and it does not contain which are the partition and sort keys.
func (s *Store) retrievePrimaryKeys() ([]string, error) {
	output, err := s.dynamoDBClient.DescribeTable(&dynamodb.DescribeTableInput{
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
			return nil, fmt.Errorf("key does not have attribute name, key: %v", key.String())
		}
	}

	return keys, nil
}
