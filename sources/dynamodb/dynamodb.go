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
	streamArn               string
	offsetFilePath          string
	lastProcessedSeqNumbers map[string]string
	streams                 *dynamodbstreams.DynamoDBStreams
}

const flushOffsetInterval = 1 * time.Minute

func Load(ctx context.Context, dynamoDB config.DynamoDB) *Store {
	sess, err := session.NewSession(&aws.Config{
		Region: ptr.ToString(dynamoDB.AwsRegion),
	})

	if err != nil {
		log.Fatalf("Failed to create session: %v", err)
	}

	// Create a DynamoDBStreams client from just a session.
	svc := dynamodbstreams.New(sess)

	return &Store{
		streamArn:               dynamoDB.StreamArn,
		offsetFilePath:          dynamoDB.OffsetFile,
		lastProcessedSeqNumbers: make(map[string]string),
		streams:                 svc,
	}
}

func (s *Store) Run(ctx context.Context) {
	s.loadOffsets(ctx)

	ticker := time.NewTicker(flushOffsetInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				s.saveOffsets(ctx)
			}
		}
	}()

	for {
		// Describe stream
		input := &dynamodbstreams.DescribeStreamInput{
			StreamArn: aws.String(s.streamArn),
		}

		result, err := s.streams.DescribeStream(input)
		if err != nil {
			log.Fatalf("Failed to describe stream: %v", err)
		}

		// Go through each shard
		for _, shard := range result.StreamDescription.Shards {
			iteratorType := "TRIM_HORIZON"
			startingSequenceNumber := ""

			if seqNumber, exists := s.lastProcessedSeqNumbers[*shard.ShardId]; exists {
				iteratorType = "AFTER_SEQUENCE_NUMBER"
				startingSequenceNumber = seqNumber
			}

			// Get shard iterator
			iteratorInput := &dynamodbstreams.GetShardIteratorInput{
				StreamArn:         ptr.ToString(s.streamArn),
				ShardId:           shard.ShardId,
				ShardIteratorType: ptr.ToString(iteratorType),
				SequenceNumber:    ptr.ToString(startingSequenceNumber),
			}

			iteratorOutput, err := s.streams.GetShardIterator(iteratorInput)
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

				getRecordsOutput, err := s.streams.GetRecords(getRecordsInput)
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

				if len(getRecordsOutput.Records) > 0 {
					lastRecord := getRecordsOutput.Records[len(getRecordsOutput.Records)-1]
					s.lastProcessedSeqNumbers[*shard.ShardId] = *lastRecord.Dynamodb.SequenceNumber
				}

				shardIterator = getRecordsOutput.NextShardIterator
			}
		}

		// Sleep for a defined interval before checking again
		// TODO - Clean up
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
