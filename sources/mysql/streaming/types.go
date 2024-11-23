package streaming

import (
	"fmt"
	"log/slog"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/maputil"
	"github.com/artie-labs/reader/lib/storage/persistedmap"
	"github.com/artie-labs/reader/sources/mysql/adapter"
)

type Position struct {
	File string `yaml:"file"`
	Pos  uint32 `yaml:"pos"`
}

func (p Position) String() string {
	return fmt.Sprintf("File: %s, Pos: %d", p.File, p.Pos)
}

func (p Position) ToMySQLPosition() mysql.Position {
	return mysql.Position{Name: p.File, Pos: p.Pos}
}

type Iterator struct {
	batchSize             int32
	offsets               *persistedmap.PersistedMap[Position]
	position              Position
	includedTablesAdapter map[string]*maputil.MostRecentMap[adapter.Table]
	streamer              *replication.BinlogStreamer
	syncer                *replication.BinlogSyncer

	// TODO
	schemaHistory *persistedmap.PersistedMap[any]
}

func BuildStreamingIterator(cfg config.MySQL) (*Iterator, error) {
	var pos Position
	offsets := persistedmap.NewPersistedMap[Position](cfg.StreamingSettings.OffsetFile)
	if _pos, isOk := offsets.Get(offsetKey); isOk {
		slog.Info("Found offsets", slog.String("offset", _pos.String()))
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

	return &Iterator{
		batchSize:             cfg.GetStreamingBatchSize(),
		position:              pos,
		syncer:                syncer,
		streamer:              streamer,
		includedTablesAdapter: includedTablesAdapter,
		offsets:               offsets,
	}, nil
}
