package postgres

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/segmentio/kafka-go"
)

func Run(ctx context.Context, cfg config.Settings, statsD *mtr.Client, kafkaWriter *kafka.Writer) {
	batchWriter := kafkalib.NewBatchWriter(ctx, *cfg.Kafka, kafkaWriter)

	db, err := sql.Open("postgres", NewConnection(cfg.PostgreSQL).String())
	if err != nil {
		logger.Fatal("Failed to connect to postgres", slog.Any("err", err))
	}
	defer db.Close()

	for _, table := range cfg.PostgreSQL.Tables {
		snapshotStartTime := time.Now()
		iter, err := LoadTable(db, table, statsD, cfg.Kafka.MaxRequestSize)
		if err != nil {
			logger.Fatal("Failed to create table iterator", slog.Any("err", err), slog.String("table", table.Name))
		}

		var count int
		for iter.HasNext() {
			msgs, err := iter.Next()
			if err != nil {
				logger.Fatal("Failed to iterate over table", slog.Any("err", err), slog.String("table", table.Name))
			} else if len(msgs) > 0 {
				if err = batchWriter.Write(msgs); err != nil {
					logger.Fatal("Failed to write messages to kafka", slog.Any("err", err), slog.String("table", table.Name))
				}
				count += len(msgs)
				slog.Info("Scanning progress", slog.Duration("timing", time.Since(snapshotStartTime)), slog.Int("count", count))
			}
		}

		slog.Info("Finished snapshotting",
			slog.Int("scannedTotal", count),
			slog.Duration("totalDuration", time.Since(snapshotStartTime)),
		)
	}
}
