package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/debezium/transformer"
	"github.com/artie-labs/reader/lib/rdbms"
	"github.com/artie-labs/reader/lib/writer"
	"github.com/artie-labs/reader/sources/postgres/adapter"
)

type Source struct {
	cfg config.PostgreSQL
	db  *sql.DB
}

func Load(cfg config.PostgreSQL) (*Source, error) {
	db, err := sql.Open("pgx", cfg.ToDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	return &Source{
		cfg: cfg,
		db:  db,
	}, nil
}

func (s *Source) Close() error {
	return s.db.Close()
}

func (s *Source) Run(ctx context.Context, _writer writer.Writer) error {
	for _, tableCfg := range s.cfg.Tables {
		logger := slog.With(slog.String("schema", tableCfg.Schema), slog.String("table", tableCfg.Name))
		snapshotStartTime := time.Now()

		dbzAdapter, err := adapter.NewPostgresAdapter(s.db, *tableCfg)
		if err != nil {
			return fmt.Errorf("failed to create PostgreSQL adapter: %w", err)
		}

		dbzTransformer, err := transformer.NewDebeziumTransformer(dbzAdapter)
		if err != nil {
			if errors.Is(err, rdbms.ErrNoPkValuesForEmptyTable) {
				logger.Info("Table does not contain any rows, skipping...")
				continue
			} else {
				return fmt.Errorf("failed to build Debezium transformer for table %s: %w", tableCfg.Name, err)
			}
		}

		logger.Info("Scanning table...", slog.Any("batchSize", tableCfg.GetBatchSize()))
		count, err := _writer.Write(ctx, dbzTransformer)
		if err != nil {
			return fmt.Errorf("failed to snapshot for table %s: %w", tableCfg.Name, err)
		}

		logger.Info("Finished snapshotting",
			slog.Int("scannedTotal", count),
			slog.Duration("totalDuration", time.Since(snapshotStartTime)),
		)
	}

	return nil
}
