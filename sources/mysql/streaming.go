package mysql

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/typing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/storage/persistedmap"
	"github.com/artie-labs/reader/writers"
)

const offsetKey = "offset"

type StreamingPosition struct {
	File string `yaml:"file"`
	Pos  uint32 `yaml:"pos"`
}

func (s StreamingPosition) buildMySQLPosition() mysql.Position {
	return mysql.Position{Name: s.File, Pos: s.Pos}
}

type Streaming struct {
	syncer   *replication.BinlogSyncer
	offsets  *persistedmap.PersistedMap
	position StreamingPosition
}

func (s Streaming) Close() error {
	s.syncer.Close()
	return nil
}

func buildStreamingConfig(cfg config.MySQL) (Streaming, error) {
	streaming := Streaming{
		syncer: replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
			ServerID: cfg.StreamingSettings.ServerID,
			Flavor:   "mysql",
			Host:     cfg.Host,
			Port:     uint16(cfg.Port),
			User:     cfg.Username,
			Password: cfg.Password,
		}),
	}

	storage := persistedmap.NewPersistedMap(cfg.StreamingSettings.OffsetFile)
	value, isOk := storage.Get(offsetKey)
	if isOk {
		pos, err := typing.AssertType[StreamingPosition](value)
		if err != nil {
			return Streaming{}, err
		}

		streaming.position = pos
	}

	return streaming, nil
}

func (s Streaming) Run(ctx context.Context, writer writers.Writer) error {
	streamer, err := s.syncer.StartSync(s.position.buildMySQLPosition())
	if err != nil {
		return fmt.Errorf("failed to start sync: %w", err)
	}

	for {
		evt, err := streamer.GetEvent(ctx)
		if err != nil {
			return fmt.Errorf("failed to get event: %w", err)
		}
	}

	return nil
}
