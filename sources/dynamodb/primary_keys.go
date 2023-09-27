package dynamodb

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (s *Store) RetrievePrimaryKeys() ([]string, error) {
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
