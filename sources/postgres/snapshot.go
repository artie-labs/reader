package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/lib/pq"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/reader/lib/postgres"
)

const defaultErrorRetries = 10

func Run(ctx context.Context, cfg config.Settings, statsD *mtr.Client, writer kafkalib.BatchWriter) error {
	db, err := sql.Open("postgres", postgres.NewConnection(cfg.PostgreSQL).String())
	if err != nil {
		return fmt.Errorf("failed to connect to postgres, err: %w", err)
	}
	defer db.Close()

	for _, tableCfg := range cfg.PostgreSQL.Tables {
		snapshotStartTime := time.Now()

		slog.Info("Loading configuration for table", slog.String("table", tableCfg.Name))
		table := postgres.NewTable(tableCfg)
		if err := table.RetrieveColumns(db); err != nil {
			if postgres.NoRowsError(err) {
				slog.Info("Table does not contain any rows, skipping...", slog.String("table", table.Name))
				continue
			} else {
				return fmt.Errorf("failed to load table configuration, table: %s, err: %w", table.Name, err)
			}
		}

		slog.Info("Scanning table",
			slog.String("tableName", table.Name),
			slog.String("schemaName", table.Schema),
			slog.String("topicSuffix", table.TopicSuffix()),
			slog.Any("primaryKeyColumns", table.PrimaryKeys.Keys()),
			slog.Any("batchSize", tableCfg.GetBatchSize()),
		)

		scanner := table.NewScanner(db, tableCfg.GetBatchSize(), defaultErrorRetries)
		messageBuilder := postgres.NewMessageBuilder(table, &scanner, statsD, cfg.Kafka.MaxRequestSize)
		count, err := writer.WriteIterator(ctx, messageBuilder)
		if err != nil {
			return fmt.Errorf("failed to snapshot, table: %s, err: %w", table.Name, err)
		}

		slog.Info("Finished snapshotting",
			slog.Int("scannedTotal", count),
			slog.Duration("totalDuration", time.Since(snapshotStartTime)),
		)
	}

	return nil
}
