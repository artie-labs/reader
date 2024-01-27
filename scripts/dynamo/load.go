package main

import (
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

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

	numRows, err := strconv.Atoi(os.Args[1])
	if err != nil || numRows < 1 {
		logger.Fatal("Please provide a valid number for rows")
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		logger.Fatal("Failed to create session", slog.Any("err", err))
	}

	svc := dynamodb.New(sess)

	// Splitting the items into batches
	for i := 0; i < numRows; i += maxBatchSize {
		var writeRequests []*dynamodb.WriteRequest
		accountID := fmt.Sprintf("account-%d", i)
		// For each batch, prepare the items
		for j := 0; j < maxBatchSize && (i+j) < numRows; j++ {
			userID := fmt.Sprintf("user_id_%v", j)
			item := map[string]*dynamodb.AttributeValue{
				"account_id": {
					S: aws.String(accountID),
				},
				"user_id": {
					S: aws.String(userID),
				},
				"random_number": {
					N: aws.String(fmt.Sprintf("%v", rand.Int63())), // Example number
				},
				"flag": {
					BOOL: aws.Bool(rand.Intn(2) == 0), // Randomly true or false
				},
				"is_null": {
					NULL: aws.Bool(true), // Will always be Null
				},
				"string_set": {
					SS: []*string{aws.String("value1"), aws.String("value2"), aws.String("value44"), aws.String("value55"), aws.String("value66")},
				},
				"number_set": {
					NS: []*string{aws.String("1"), aws.String("2"), aws.String("3")},
				},
				"sample_list": {
					L: []*dynamodb.AttributeValue{
						{
							S: aws.String("item1"),
						},
						{
							N: aws.String("2"),
						},
					},
				},
				"sample_map": {
					M: map[string]*dynamodb.AttributeValue{
						"key1": {
							S: aws.String("value1"),
						},
						"key2": {
							N: aws.String("2"),
						},
					},
				},
			}

			writeRequest := &dynamodb.WriteRequest{
				PutRequest: &dynamodb.PutRequest{
					Item: item,
				},
			}
			writeRequests = append(writeRequests, writeRequest)
		}

		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				table: writeRequests,
			},
		}

		_, err := svc.BatchWriteItem(input)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to write batch starting at index %d", i), slog.Any("err", err))
			continue
		}

		slog.Info(fmt.Sprintf("Inserted batch of items starting from index %d", i))
	}
}
