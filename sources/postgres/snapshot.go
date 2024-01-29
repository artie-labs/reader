package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/reader/lib/postgres"
)

const defaultErrorRetries = 10

func loadTable(db *sql.DB, tableCfg *config.PostgreSQLTable, statsD *mtr.Client, maxRowSize uint64) (*postgres.MessageBuilder, error) {
	slog.Info("Loading configuration for table", slog.String("table", tableCfg.Name))

	table := postgres.NewTable(tableCfg)
	if err := table.RetrieveColumns(db); err != nil {
		if postgres.NoRowsError(err) {
			slog.Info("Table does not contain any rows, skipping...", slog.String("table", table.Name))
			return nil, nil
		} else {
			return nil, fmt.Errorf("failed to validate postgres: %w", err)
		}
	}

	slog.Info("Scanning table",
		slog.String("tableName", table.Name),
		slog.String("schemaName", table.Schema),
		slog.String("topicSuffix", table.TopicSuffix()),
		slog.Any("primaryKeyColumns", table.PrimaryKeys.Keys()),
		slog.Any("batchSize", tableCfg.GetLimit()),
	)

	scanner := table.NewScanner(db, tableCfg.GetLimit(), defaultErrorRetries)
	return postgres.NewMessageBuilder(
		table,
		&scanner,
		statsD,
		maxRowSize,
	), nil
}

func Run(ctx context.Context, cfg config.Settings, statsD *mtr.Client, kafkaWriter *kafka.Writer) error {
	batchWriter := kafkalib.NewBatchWriter(ctx, *cfg.Kafka, kafkaWriter)

	db, err := sql.Open("postgres", postgres.NewConnection(cfg.PostgreSQL).String())
	if err != nil {
		return fmt.Errorf("failed to connect to postgres, err: %w", err)
	}
	defer db.Close()

	for _, table := range cfg.PostgreSQL.Tables {
		snapshotStartTime := time.Now()
		iter, err := loadTable(db, table, statsD, cfg.Kafka.MaxRequestSize)
		if err != nil {
			return fmt.Errorf("failed to create table iterator, table: %s, err: %w", table.Name, err)
		}

		count, err := batchWriter.WriteIterable(iter)
		if err != nil {
			return fmt.Errorf("failed to write messages to kafka, table: %s, err: %w", table.Name, err)
		}

		slog.Info("Finished snapshotting",
			slog.Int("scannedTotal", count),
			slog.Duration("totalDuration", time.Since(snapshotStartTime)),
		)
	}

	return nil
}
