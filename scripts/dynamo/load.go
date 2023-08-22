package main

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	region = "us-east-1"
	table  = "ddb-test"
)

func randomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s <number_of_rows>", os.Args[0])
	}

	numRows, err := strconv.Atoi(os.Args[1])
	if err != nil || numRows < 1 {
		log.Fatalf("Please provide a valid number for rows")
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		log.Fatalf("Failed to create session: %v", err)
	}

	svc := dynamodb.New(sess)
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < numRows; i++ {
		accountID := randomString(10)
		userID := randomString(5)

		item := map[string]*dynamodb.AttributeValue{
			"account_id": {
				S: aws.String(accountID),
			},
			"user_id": {
				S: aws.String(userID),
			},
		}

		input := &dynamodb.PutItemInput{
			TableName: aws.String(table),
			Item:      item,
		}

		_, err := svc.PutItem(input)
		if err != nil {
			log.Printf("Failed to put item for accountID: %s, userID: %s. Error: %v", accountID, userID, err)
			continue
		}

		log.Printf("Inserted data for accountID: %s, userID: %s", accountID, userID)
	}
}
