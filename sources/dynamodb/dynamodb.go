package dynamodb

import (
	"bufio"
	"context"
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/dynamo"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
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

const (
	flushOffsetInterval = 1 * time.Minute

	// jitterSleepBaseMs - sleep for 5s as the base.
	jitterSleepBaseMs = 5000
)

func Load(ctx context.Context) *Store {
	cfg := config.FromContext(ctx)
	sess, err := session.NewSession(&aws.Config{
		Region: ptr.ToString(cfg.DynamoDB.AwsRegion),
	})

	if err != nil {
		logger.FromContext(ctx).Fatalf("Failed to create session: %v", err)
	}

	return &Store{
		streamArn:               cfg.DynamoDB.StreamArn,
		offsetFilePath:          cfg.DynamoDB.OffsetFile,
		lastProcessedSeqNumbers: make(map[string]string),
		streams:                 dynamodbstreams.New(sess),
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

	log := logger.FromContext(ctx)
	for {
		input := &dynamodbstreams.DescribeStreamInput{
			StreamArn: aws.String(s.streamArn),
		}

		result, err := s.streams.DescribeStream(input)
		if err != nil {
			log.Fatalf("Failed to describe stream: %v", err)
		}

		var retrievedMessages bool
		var attempts int
		for _, shard := range result.StreamDescription.Shards {
			iteratorType := "TRIM_HORIZON"
			var startingSequenceNumber string

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
				log.WithError(err).WithFields(map[string]interface{}{
					"streamArn": s.streamArn,
					"shardId":   *shard.ShardId,
				}).Warn("failed to get shard iterator...")
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
					retrievedMessages = true
					lastRecord := getRecordsOutput.Records[len(getRecordsOutput.Records)-1]
					s.lastProcessedSeqNumbers[*shard.ShardId] = *lastRecord.Dynamodb.SequenceNumber
				}

				shardIterator = getRecordsOutput.NextShardIterator
			}
		}

		if !retrievedMessages {
			attempts += 1
			sleepDuration := time.Duration(jitter.JitterMs(jitterSleepBaseMs, attempts)) * time.Millisecond
			log.WithFields(map[string]interface{}{
				"streamArn":     s.streamArn,
				"sleepDuration": sleepDuration,
				"attempts":      attempts,
			}).Info("No messages retrieved this iteration, sleeping and will retry again")

			time.Sleep(sleepDuration)
		} else {
			attempts = 0
		}
	}
}

func (s *Store) loadOffsets(ctx context.Context) {
	log := logger.FromContext(ctx)
	log.Infof("loading DynamoDB offsets from file: %s", s.offsetFilePath)
	file, err := os.Open(s.offsetFilePath)
	if err != nil {
		log.WithError(err).Warn("failed to open DynamoDB offset file, so not using previously stored offsets...")
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
		logger.FromContext(ctx).WithError(err).Fatal("failed to create DynamoDB offset file")
	}

	defer file.Close()

	writer := bufio.NewWriter(file)
	for shardID, sequenceNumber := range s.lastProcessedSeqNumbers {
		_, err = writer.WriteString(fmt.Sprintf("%s:%s\n", shardID, sequenceNumber))
		if err != nil {
			logger.FromContext(ctx).WithError(err).Fatal("failed to write to DynamoDB offset file")
			continue
		}
	}

	_ = writer.Flush()
}
