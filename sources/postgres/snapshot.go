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

func Run(ctx context.Context, cfg config.Settings, statsD *mtr.Client, kafkaWriter *kafka.Writer) error {
	batchWriter := kafkalib.NewBatchWriter(ctx, *cfg.Kafka, kafkaWriter)

	db, err := sql.Open("postgres", postgres.NewConnection(cfg.PostgreSQL).String())
	if err != nil {
		return fmt.Errorf("failed to connect to postgres, err: %v", err)
	}
	defer db.Close()

	for _, table := range cfg.PostgreSQL.Tables {
		snapshotStartTime := time.Now()
		iter, err := postgres.LoadTable(db, table, statsD, cfg.Kafka.MaxRequestSize)
		if err != nil {
			return fmt.Errorf("failed to create table iterator, table: %s, err: %v", table.Name, err)
		}

		var count int
		for iter.HasNext() {
			msgs, err := iter.Next()
			if err != nil {
				return fmt.Errorf("failed to iterate over table, table: %s, err: %v", table.Name, err)

			} else if len(msgs) > 0 {
				if err = batchWriter.Write(msgs); err != nil {
					return fmt.Errorf("failed to write messages to kafka, table: %s, err: %v", table.Name, err)
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

	return nil
}
