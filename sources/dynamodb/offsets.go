package dynamodb

import (
	"bufio"
	"context"
	"fmt"
	"github.com/artie-labs/reader/lib/logger"
	"os"
	"strings"
)

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
