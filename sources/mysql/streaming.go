package mysql

import (
	"context"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/writers"
)

type Streaming struct {
	syncer *replication.BinlogSyncer
}

func (s Streaming) Close() error {
	return nil
}

func buildStreamingConfig(cfg config.MySQL) (Streaming, error) {
	syncerConfig := replication.BinlogSyncerConfig{
		ServerID: cfg.StreamingSettings.ServerID,
		Flavor:   "mysql",
		Host:     cfg.Host,
		Port:     uint16(cfg.Port),
		User:     cfg.Username,
		Password: cfg.Password,
	}

	return Streaming{syncer: replication.NewBinlogSyncer(syncerConfig)}, nil
}

func (s Streaming) Run(ctx context.Context, writer writers.Writer) error {
	return nil
}