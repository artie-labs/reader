package streaming

import (
	"fmt"
	"log/slog"

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
	// TODO
	return nil, nil
}
