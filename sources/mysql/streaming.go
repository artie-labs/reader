package mysql

import (
	"context"
	"log/slog"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/storage/persistedmap"
	"github.com/artie-labs/reader/sources/mysql/streaming"
	"github.com/artie-labs/reader/writers"
)

const offsetKey = "offset"

type Streaming struct {
	offsets           *persistedmap.PersistedMap[streaming.Position]
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
		offsets: persistedmap.NewPersistedMap[streaming.Position](cfg.StreamingSettings.OffsetFile),
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

	position, isOk := streamer.offsets.Get(offsetKey)
	if isOk {
		slog.Info("Loaded offsets", slog.String("offset", position.String()))
		streamer.position = position
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
