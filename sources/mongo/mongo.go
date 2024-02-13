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

type Source struct {
	cfg config.MongoDB
	db  *mongo.Database
}

func Load(cfg config.MongoDB) (*Source, error) {
	creds := options.Credential{
		Username: cfg.Username,
		Password: cfg.Password,
	}

	opts := options.Client().ApplyURI(cfg.Host).SetAuth(creds).SetTLSConfig(&tls.Config{})
	client, err := mongo.Connect(context.Background(), opts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mongodb, err: %w", err)
	}

	if err = client.Ping(context.TODO(), readpref.Primary()); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB, err: %w", err)
	}

	db := client.Database(cfg.Database)
	return &Source{
		cfg: cfg,
		db:  db,
	}, nil
}

func (s *Source) Close() error {
	// MongoDB doesn't need to be closed.
	return nil
}

func (s *Source) Run(ctx context.Context, writer kafkalib.BatchWriter, statsD *mtr.Client) error {
	for _, collection := range s.cfg.Collections {
		snapshotStartTime := time.Now()

		slog.Info("Scanning collection",
			slog.String("collectionName", collection.Name),
			slog.String("topicSuffix", collection.TopicSuffix()),
			slog.Any("batchSize", collection.GetBatchSize()),
		)

		iterator := newIterator(s.db, collection)
		count, err := writer.WriteIterator(ctx, iterator)
		if err != nil {
			return fmt.Errorf("failed to snapshot, table: %s, err: %w", collection.Name, err)
		}

		slog.Info("Finished snapshotting", slog.Int("scannedTotal", count), slog.Duration("totalDuration", time.Since(snapshotStartTime)))
	}

	return nil
}
