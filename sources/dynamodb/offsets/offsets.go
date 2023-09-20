package offsets

import (
	"context"
	"fmt"
	"github.com/artie-labs/reader/lib/ttlmap"
	"time"
)

const ShardExpirationAndBuffer = 26 * time.Hour

type OffsetStorage struct {
	ttlMap *ttlmap.TTLMap
}

func shardProcessingKey(shardId string) string {
	return fmt.Sprintf("processing#shardId#%s", shardId)
}

func shardProcessKey(shardId string) string {
	return fmt.Sprintf("processed#shardId#%s", shardId)
}

func shardSeqNumberKey(shardId string) string {
	return fmt.Sprintf("seqNumber#shardId#%s", shardId)
}

func (o *OffsetStorage) SetShardProcessed(shardID string) {
	o.ttlMap.Set(ttlmap.SetArgs{
		Key:   shardProcessKey(shardID),
		Value: true,
	}, ShardExpirationAndBuffer)
}

func (o *OffsetStorage) GetShardProcessed(shardID string) bool {
	_, isOk := o.ttlMap.Get(shardProcessKey(shardID))
	return isOk
}

// SetShardProcessing sets the shard processing flag for the given shardID
// This is used so that we don't process the same shard twice
func (o *OffsetStorage) SetShardProcessing(shardID string) {
	o.ttlMap.Set(ttlmap.SetArgs{
		Key:   shardProcessingKey(shardID),
		Value: true,
		// Don't flush this to disk
		// This is only used to alleviate shard contention and prevent memory leak by having built-in GC.
		DoNotFlushToDisk: true,
	}, ShardExpirationAndBuffer)
}

func (o *OffsetStorage) GetShardProcessing(shardID string) bool {
	_, isOk := o.ttlMap.Get(shardProcessingKey(shardID))
	return isOk
}

func (o *OffsetStorage) SetLastProcessedSequenceNumber(shardID string, sequenceNumber string) {
	o.ttlMap.Set(ttlmap.SetArgs{
		Key:   shardSeqNumberKey(shardID),
		Value: sequenceNumber,
	}, ShardExpirationAndBuffer)
}

func (o *OffsetStorage) LastProcessedSequenceNumber(shardID string) (string, bool) {
	sequenceNumber, isOk := o.ttlMap.Get(shardSeqNumberKey(shardID))
	if !isOk {
		return "", false
	}

	return fmt.Sprint(sequenceNumber), true
}

func NewStorage(ctx context.Context, fp string, cleanUpIntervalOverride, flushIntervalOverride *time.Duration) *OffsetStorage {
	cleanUpInterval := ttlmap.DefaultCleanUpInterval
	if cleanUpIntervalOverride != nil {
		cleanUpInterval = *cleanUpIntervalOverride
	}

	flushInterval := ttlmap.DefaultFlushInterval
	if flushIntervalOverride != nil {
		flushInterval = *flushIntervalOverride
	}

	offset := &OffsetStorage{
		ttlMap: ttlmap.NewMap(ctx, fp, cleanUpInterval, flushInterval),
	}
	return offset
}
