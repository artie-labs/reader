package offsets

import (
	"bufio"
	"context"
	"fmt"
	"github.com/artie-labs/reader/lib/logger"
	"os"
	"strings"
	"sync"
)

type OffsetStorage struct {
	lastProcessedSeqNumbers map[string]string
	shouldSave              bool
	fp                      string
	sync.Mutex
}

func (o *OffsetStorage) SetLastProcessedSequenceNumber(shardID string, sequenceNumber string) {
	o.Lock()
	defer o.Unlock()
	o.lastProcessedSeqNumbers[shardID] = sequenceNumber
	o.shouldSave = true
}

func (o *OffsetStorage) ReadOnlyLastProcessedSequenceNumbers(shardID string) (string, bool) {
	o.Lock()
	defer o.Unlock()

	val, isOk := o.lastProcessedSeqNumbers[shardID]
	return val, isOk
}

func NewStorage(ctx context.Context, fp string) *OffsetStorage {
	offset := &OffsetStorage{
		lastProcessedSeqNumbers: make(map[string]string),
		fp:                      fp,
	}

	offset.load(ctx)
	return offset
}

func (o *OffsetStorage) load(ctx context.Context) {
	log := logger.FromContext(ctx)
	log.Infof("loading DynamoDB offsets from file: %s", o.fp)
	file, err := os.Open(o.fp)
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
			o.lastProcessedSeqNumbers[shardID] = sequenceNumber
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Error reading offset file: %v", err)
	}
}

func (o *OffsetStorage) Save(ctx context.Context) {
	o.Lock()
	defer o.Unlock()

	if !o.shouldSave {
		return
	}

	file, err := os.Create(o.fp)
	if err != nil {
		logger.FromContext(ctx).WithError(err).Fatal("failed to create DynamoDB offset file")
	}

	defer file.Close()

	writer := bufio.NewWriter(file)
	for shardID, sequenceNumber := range o.lastProcessedSeqNumbers {
		_, err = writer.WriteString(fmt.Sprintf("%s:%s\n", shardID, sequenceNumber))
		if err != nil {
			logger.FromContext(ctx).WithError(err).Fatal("failed to write to DynamoDB offset file")
			continue
		}
	}

	_ = writer.Flush()
	o.shouldSave = false
}
