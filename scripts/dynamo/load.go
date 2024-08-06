package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/artie-labs/reader/lib/logger"
)

const (
	region       = "us-east-1"
	table        = "ddb-test"
	maxBatchSize = 25 // DynamoDB's limit for batch write
	offset       = 500000
)

func main() {
	if len(os.Args) != 2 {
		logger.Fatal(fmt.Sprintf("Usage: %s <num_batches>", os.Args[0]))
	}

	ctx := context.Background()
	numBatches, err := strconv.Atoi(os.Args[1])
	if err != nil || numBatches < 1 {
		logger.Fatal("Please provide a valid number for rows")
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		logger.Fatal("Failed to load AWS config", slog.Any("err", err))
	}

	svc := dynamodb.NewFromConfig(awsCfg)

	var rowsWritten int
	// Splitting the items into batches
	for i := offset + 0; i < offset+numBatches; i += maxBatchSize {
		var writeRequests []types.WriteRequest
		accountID := fmt.Sprintf("account-%d", i)
		// For each batch, prepare the items
		for j := 0; j < maxBatchSize; j++ {
			userID := fmt.Sprintf("user_id_%v", j)
			item := map[string]types.AttributeValue{
				"account_id": &types.AttributeValueMemberS{
					Value: accountID,
				},
				"user_id": &types.AttributeValueMemberS{
					Value: userID,
				},
				"b": &types.AttributeValueMemberB{
					Value: []byte("hello world"),
				},
				"bs": &types.AttributeValueMemberBS{
					Value: [][]byte{[]byte("hello"), []byte("world")},
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

		rowsWritten += len(writeRequests)
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				table: writeRequests,
			},
		}

		if _, err = svc.BatchWriteItem(ctx, input); err != nil {
			slog.Error(fmt.Sprintf("Failed to write batch starting at index %d", i), slog.Any("err", err))
			continue
		}

		// Our test DDB has low WCUs
		time.Sleep(2 * time.Second)
		rowsWritten += len(writeRequests)
		slog.Info(fmt.Sprintf("Inserted batch of items starting from index %d", i))
	}

	slog.Info("Successfully inserted all items", slog.Int("num_rows", rowsWritten))
}
