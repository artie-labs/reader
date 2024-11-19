package streaming

import (
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/iterator"
	"github.com/artie-labs/reader/lib/maputil"
	"github.com/artie-labs/reader/lib/storage/persistedmap"
	"github.com/artie-labs/reader/sources/mysql/adapter"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/go-mysql-org/go-mysql/replication"
	"log/slog"
)

const offsetKey = "offset"

type Iterator struct {
	offsets               *persistedmap.PersistedMap
	streamer              *replication.BinlogStreamer
	position              Position
	includedTablesAdapter map[string]*maputil.MostRecentMap[adapter.Table]

	// TODO
	schemaHistory *persistedmap.PersistedMap
}

func (i *Iterator) Next() ([]lib.RawMessage, error) {
	//TODO implement me
	panic("implement me")
}

func (i *Iterator) shouldProcessTable(tableName []byte) bool {
	_, isOk := i.includedTablesAdapter[string(tableName)]
	return isOk
}

func (i *Iterator) HasNext() bool {
	return true
}

func (i *Iterator) CommitOffset() {
	i.offsets.Set(offsetKey, i.position)
	slog.Info("Committing offset", slog.Any("position", i.position))
}

func BuildStreamingIterator(cfg config.MySQL) (iterator.StreamingIterator[[]lib.RawMessage], error) {
	var pos Position
	offsets := persistedmap.NewPersistedMap(cfg.StreamingSettings.OffsetFile)
	value, isOk := offsets.Get(offsetKey)
	if isOk {
		_pos, err := typing.AssertType[Position](value)
		if err != nil {
			return nil, err
		}

		slog.Info("Found offsets", slog.String("offset", pos.String()))
		pos = _pos
	}

	includedTablesAdapter, err := BuildTablesAdapter(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to build schema history: %w", err)
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
		return nil, fmt.Errorf("failed to start sync: %w", err)
	}

	return Iterator{
		position:              pos,
		streamer:              streamer,
		includedTablesAdapter: includedTablesAdapter,
		offsets:               offsets,
	}, nil
}
