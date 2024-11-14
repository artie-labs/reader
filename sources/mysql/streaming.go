package mysql

import "github.com/artie-labs/reader/config"

func buildStreamingConfig(cfg config.MySQL) (*StreamingSource, error) {
	syncerConfig := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     cfg.Host,
		Port:     uint16(cfg.Port),
		User:     cfg.Username,
		Password: cfg.Password,
	}

	return &StreamingSource{
		cfg:    cfg,
		syncer: replication.NewBinlogSyncer(syncerConfig),
	}, nil
}
