package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/reader/lib/postgres"
	"github.com/artie-labs/reader/sources"
)

const defaultErrorRetries = 10

type postgresSource struct {
	cfg        config.PostgreSQL
	maxRowSize uint64
	db         *sql.DB
}

func Load(cfg config.PostgreSQL, maxRowSize uint64) (sources.Source, error) {
	db, err := sql.Open("pgx", postgres.NewConnection(cfg).String())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	return &postgresSource{
		cfg:        cfg,
		maxRowSize: maxRowSize,
		db:         db,
	}, nil
}

func (p postgresSource) Close() error {
	return p.db.Close()
}

func (p postgresSource) Run(ctx context.Context, writer kafkalib.BatchWriter, statsD *mtr.Client) error {
	for _, tableCfg := range p.cfg.Tables {
		snapshotStartTime := time.Now()

		slog.Info("Loading configuration for table", slog.String("table", tableCfg.Name))
		table := postgres.NewTable(tableCfg)
		if err := table.RetrieveColumns(p.db); err != nil {
			if postgres.NoRowsError(err) {
				slog.Info("Table does not contain any rows, skipping...", slog.String("table", table.Name))
				continue
			} else {
				return fmt.Errorf("failed to load configuration for table %s: %w", table.Name, err)
			}
		}

		slog.Info("Scanning table",
			slog.String("tableName", table.Name),
			slog.String("schemaName", table.Schema),
			slog.String("topicSuffix", table.TopicSuffix()),
			slog.Any("primaryKeyColumns", table.PrimaryKeys.Keys()),
			slog.Any("batchSize", tableCfg.GetBatchSize()),
		)

		scanner := table.NewScanner(p.db, tableCfg.GetBatchSize(), defaultErrorRetries)
		messageBuilder := postgres.NewMessageBuilder(table, &scanner, statsD, p.maxRowSize)
		count, err := writer.WriteIterator(ctx, messageBuilder)
		if err != nil {
			return fmt.Errorf("failed to snapshot for table %s: %w", table.Name, err)
		}

		slog.Info("Finished snapshotting",
			slog.Int("scannedTotal", count),
			slog.Duration("totalDuration", time.Since(snapshotStartTime)),
		)
	}

	return nil
}
