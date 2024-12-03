package streaming

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/storage/persistedmap"
)

const offsetKey = "offset"

func BuildStreamingIterator(cfg config.MySQL) (Iterator, error) {
	var pos Position
	offsets := persistedmap.NewPersistedMap[Position](cfg.StreamingSettings.OffsetFile)
	if _pos, isOk := offsets.Get(offsetKey); isOk {
		slog.Info("Found offsets", slog.String("offset", _pos.String()))
		pos = _pos
	}

	syncer := replication.NewBinlogSyncer(
		replication.BinlogSyncerConfig{
			ServerID: cfg.StreamingSettings.ServerID,
			Flavor:   "mysql",
			Host:     cfg.Host,
			Port:     uint16(cfg.Port),
			User:     cfg.Username,
			Password: cfg.Password,
		},
	)

	streamer, err := syncer.StartSync(pos.ToMySQLPosition())
	if err != nil {
		return Iterator{}, fmt.Errorf("failed to start sync: %w", err)
	}

	return Iterator{
		batchSize: cfg.GetStreamingBatchSize(),
		position:  pos,
		syncer:    syncer,
		streamer:  streamer,
		offsets:   offsets,
	}, nil
}

func (i *Iterator) HasNext() bool {
	return true
}

func (i *Iterator) CommitOffset() {
	slog.Info("Committing offset", slog.String("position", i.position.String()))
	i.offsets.Set(offsetKey, i.position)
}

func (i *Iterator) Close() error {
	i.syncer.Close()
	return nil
}

func (i *Iterator) Next() ([]lib.RawMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var rawMsgs []lib.RawMessage
	for i.batchSize > int32(len(rawMsgs)) {
		select {
		case <-ctx.Done():
			return rawMsgs, nil
		default:
			event, err := i.streamer.GetEvent(ctx)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return rawMsgs, nil
				}

				return nil, fmt.Errorf("failed to get binlog event: %w", err)
			}

			if err = i.position.UpdatePosition(event); err != nil {
				return nil, fmt.Errorf("failed to update position: %w", err)
			}

			switch event.Header.EventType {
			case replication.QUERY_EVENT:
			// TODO: process DDL
			case replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2:
			// TODO: process DML
			default:
				slog.Info("Skipping event", slog.Any("eventType", event.Header.EventType))
			}
		}
	}

	if len(rawMsgs) == 0 {
		// If there are no messages, let's sleep a bit before we try again
		time.Sleep(2 * time.Second)
	}

	return rawMsgs, nil
}
