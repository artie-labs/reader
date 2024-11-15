package mysql

import (
	"context"
	"fmt"

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
		position, isOk := value.(StreamingPosition)
		if !isOk {
			return Streaming{}, fmt.Errorf("failed to cast value to type StreamingPosition, type: %T", value)
		}

		streaming.position = position
	}

	return streaming, nil
}

func (s Streaming) Run(ctx context.Context, writer writers.Writer) error {
	_, err := s.syncer.StartSync(s.position.buildMySQLPosition())
	if err != nil {
		return fmt.Errorf("failed to start sync: %w", err)
	}

	return nil
}
