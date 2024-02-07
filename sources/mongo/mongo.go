package mongo

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/mtr"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log/slog"
	"time"
)

func Run(ctx context.Context, cfg config.Settings, statsD *mtr.Client, writer kafkalib.BatchWriter) error {
	creds := options.Credential{
		Username: cfg.MongoDB.Username,
		Password: cfg.MongoDB.Password,
	}

	opts := options.Client().ApplyURI(cfg.MongoDB.Host).SetAuth(creds).SetTLSConfig(&tls.Config{})
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to connect to mongodb, err: %w", err)
	}

	if err = client.Ping(context.TODO(), readpref.Primary()); err != nil {
		return fmt.Errorf("failed to ping MongoDB, err: %w", err)
	}

	db := client.Database(cfg.MongoDB.Database)
	for _, collection := range cfg.MongoDB.Collections {
		snapshotStartTime := time.Now()

		slog.Info("Scanning collection",
			slog.String("collectionName", collection.Name),
			slog.String("topicSuffix", collection.TopicSuffix()),
			slog.Any("batchSize", collection.GetBatchSize()),
		)

		iterator := newIterator(db, collection)
		count, err := writer.WriteIterator(ctx, iterator)
		if err != nil {
			return fmt.Errorf("failed to snapshot, table: %s, err: %w", collection.Name, err)
		}

		slog.Info("Finished snapshotting", slog.Int("scannedTotal", count), slog.Duration("totalDuration", time.Since(snapshotStartTime)))
	}

	return nil
}
