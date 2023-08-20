package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/dynamo"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"log"
	"os"
	"strings"
	"time"
)

type Store struct {
	awsRegion               string
	streamArn               string
	offsetFilePath          string
	lastProcessedSeqNumbers map[string]string
}

const FLUSH_INTERVAL = 10 * time.Minute

func Load(ctx context.Context, dynamoDB config.DynamoDB) *Store {
	return &Store{
		streamArn:               dynamoDB.StreamArn,
		offsetFilePath:          dynamoDB.OffsetFile,
		lastProcessedSeqNumbers: make(map[string]string),
	}
}

func (s *Store) Run(ctx context.Context) {
	s.loadOffsets(ctx)

	sess, err := session.NewSession(&aws.Config{
		Region: ptr.ToString(s.awsRegion),
	})

	if err != nil {
		log.Fatalf("Failed to create session: %v", err)
	}

	// Create a DynamoDBStreams client from just a session.
	svc := dynamodbstreams.New(sess)

	go func() {
		for {
			time.Sleep(FLUSH_INTERVAL)
			s.saveOffsets(ctx)
		}
	}()

	for {
		// Describe stream
		input := &dynamodbstreams.DescribeStreamInput{
			StreamArn: aws.String(s.streamArn), // replace with your Stream ARN
		}

		result, err := svc.DescribeStream(input)
		if err != nil {
			log.Fatalf("Failed to describe stream: %v", err)
		}

		// Go through each shard
		for _, shard := range result.StreamDescription.Shards {
			fmt.Printf("Shard ID: %s\n", *shard.ShardId)

			// Get shard iterator
			iteratorInput := &dynamodbstreams.GetShardIteratorInput{
				StreamArn:         aws.String(s.streamArn),
				ShardId:           shard.ShardId,
				ShardIteratorType: aws.String("TRIM_HORIZON"),
			}

			iteratorOutput, err := svc.GetShardIterator(iteratorInput)
			if err != nil {
				log.Printf("Failed to get shard iterator for shard %s: %v", *shard.ShardId, err)
				continue
			}

			shardIterator := iteratorOutput.ShardIterator

			// Get records from shard iterator
			for shardIterator != nil {
				getRecordsInput := &dynamodbstreams.GetRecordsInput{
					ShardIterator: shardIterator,
				}

				getRecordsOutput, err := svc.GetRecords(getRecordsInput)
				if err != nil {
					log.Printf("Failed to get records for shard iterator: %v", err)
					break
				}

				// Print the records
				for _, record := range getRecordsOutput.Records {
					msg := dynamo.NewMessage(record)
					if err = msg.Publish(); err != nil {
						log.Printf("Failed to publish message: %v", err)
					}

					fmt.Printf("Record: %v\n", record)
				}

				shardIterator = getRecordsOutput.NextShardIterator
			}
		}

		// Sleep for a defined interval before checking again
		time.Sleep(10 * time.Minute)
	}
}

func (s *Store) loadOffsets(ctx context.Context) {
	file, err := os.Open(s.offsetFilePath)
	if err != nil {
		logger.FromContext(ctx).WithError(err).Warn("failed opening offset file, so not using previously stored offsets...")
		return
	}

	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), ":")
		if len(parts) == 2 {
			shardID := parts[0]
			sequenceNumber := parts[1]
			s.lastProcessedSeqNumbers[shardID] = sequenceNumber
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Error reading offset file: %v", err)
	}
}

func (s *Store) saveOffsets(ctx context.Context) {
	file, err := os.Create(s.offsetFilePath)
	if err != nil {
		logger.FromContext(ctx).WithError(err).Fatal("failed to create offset file")
	}

	defer file.Close()

	writer := bufio.NewWriter(file)
	for shardID, sequenceNumber := range s.lastProcessedSeqNumbers {
		_, err = writer.WriteString(fmt.Sprintf("%s:%s\n", shardID, sequenceNumber))
		if err != nil {
			logger.FromContext(ctx).WithError(err).Fatal("failed to write to offset file")
			continue
		}
	}

	_ = writer.Flush()
}
