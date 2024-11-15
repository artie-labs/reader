package mysql

import (
	"context"
	"log/slog"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/storage/persistedmap"
	"github.com/artie-labs/reader/sources/mysql/streaming"
	"github.com/artie-labs/reader/writers"
)

const offsetKey = "offset"

type Streaming struct {
	syncer   *replication.BinlogSyncer
	position streaming.Position
}

func (s Streaming) Close() error {
	s.syncer.Close()
	return nil
}

func buildStreamingConfig(cfg config.MySQL) (Streaming, error) {
	streamer := Streaming{
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
	}

	storage := persistedmap.NewPersistedMap(cfg.StreamingSettings.OffsetFile)
	value, isOk := storage.Get(offsetKey)
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
