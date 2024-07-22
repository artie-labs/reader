package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/artie-labs/reader/lib/logger"
)

const (
	region       = "us-east-1"
	table        = "ddb-test"
	maxBatchSize = 25 // DynamoDB's limit for batch write
)

func main() {
	if len(os.Args) != 2 {
		logger.Fatal(fmt.Sprintf("Usage: %s <number_of_rows>", os.Args[0]))
	}

	ctx := context.Background()
	numRows, err := strconv.Atoi(os.Args[1])
	if err != nil || numRows < 1 {
		logger.Fatal("Please provide a valid number for rows")
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		logger.Fatal("Failed to load AWS config", slog.Any("err", err))
	}

	svc := dynamodb.NewFromConfig(awsCfg)

	// Splitting the items into batches
	for i := 0; i < numRows; i += maxBatchSize {
		var writeRequests []types.WriteRequest
		accountID := fmt.Sprintf("account-%d", i)
		// For each batch, prepare the items
		for j := 0; j < maxBatchSize && (i+j) < numRows; j++ {
			userID := fmt.Sprintf("user_id_%v", j)
			item := map[string]types.AttributeValue{
				"account_id": &types.AttributeValueMemberS{
					Value: accountID,
				},
				"user_id": &types.AttributeValueMemberS{
					Value: userID,
				},
				"random_number": &types.AttributeValueMemberN{
					Value: fmt.Sprintf("%v", rand.Int63()), // Example number
				},
				"flag": &types.AttributeValueMemberBOOL{
					Value: rand.Intn(2) == 0, // Randomly true or false
				},
				"is_null": &types.AttributeValueMemberNULL{
					Value: true, // Will always be Null
				},
				"string_set": &types.AttributeValueMemberSS{
					Value: []string{"value1", "value2", "value44", "value55", "value66"},
				},
				"number_set": &types.AttributeValueMemberNS{
					Value: []string{"1", "2", "3"},
				},
				"sample_list": &types.AttributeValueMemberL{
					Value: []types.AttributeValue{
						&types.AttributeValueMemberS{Value: "item1"},
						&types.AttributeValueMemberN{Value: "2"},
					},
				},
				"sample_map": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"key1": &types.AttributeValueMemberS{Value: "value1"},
						"key2": &types.AttributeValueMemberN{Value: "2"},
					},
				},
			}

			writeRequest := types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: item,
				},
			}
			writeRequests = append(writeRequests, writeRequest)
		}

		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				table: writeRequests,
			},
		}

		if _, err = svc.BatchWriteItem(ctx, input); err != nil {
			slog.Error(fmt.Sprintf("Failed to write batch starting at index %d", i), slog.Any("err", err))
			continue
		}

		slog.Info(fmt.Sprintf("Inserted batch of items starting from index %d", i))
	}
}
