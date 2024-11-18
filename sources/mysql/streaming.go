package mysql

import (
	"context"
	"github.com/artie-labs/reader/lib/maputil"
	"github.com/artie-labs/reader/sources/mysql/adapter"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/go-mysql-org/go-mysql/replication"
	"log/slog"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/storage/persistedmap"
	"github.com/artie-labs/reader/sources/mysql/streaming"
	"github.com/artie-labs/reader/writers"
)

const offsetKey = "offset"

type Streaming struct {
	schemaHistory     map[string]*maputil.MostRecentMap[adapter.Table]
	storage           *persistedmap.PersistedMap
	syncer            *replication.BinlogSyncer
	position          streaming.Position
	includedTablesMap map[string]bool
}

func (s Streaming) shouldProcessTable(tableName string) bool {
	_, isOk := s.includedTablesMap[tableName]
	return isOk
}

func (s Streaming) Close() error {
	s.syncer.Close()
	return nil
}

func buildStreamingConfig(cfg config.MySQL) (Streaming, error) {
	includedTablesMap := make(map[string]bool)
	for _, table := range cfg.Tables {
		includedTablesMap[table.Name] = true
	}

	streamer := Streaming{
		storage: persistedmap.NewPersistedMap(cfg.StreamingSettings.OffsetFile),
		syncer: replication.NewBinlogSyncer(
			replication.BinlogSyncerConfig{
				ServerID: cfg.StreamingSettings.ServerID,
				Flavor:   "mysql",
				Host:     cfg.Host,
				Port:     uint16(cfg.Port),
				User:     cfg.Username,
				Password: cfg.Password,
			},
		),
		includedTablesMap: includedTablesMap,
	}

	value, isOk := streamer.storage.Get(offsetKey)
	if isOk {
		pos, err := typing.AssertType[streaming.Position](value)
		if err != nil {
			return Streaming{}, err
		}

		slog.Info("Loaded offsets", slog.String("offset", pos.String()))
		streamer.position = pos
	}

	return streamer, nil
}

func (s Streaming) Run(ctx context.Context, writer writers.Writer) error {
	_, err := s.syncer.StartSync(s.position.ToMySQLPosition())
	if err != nil {
		return err
	}

	return nil
}
