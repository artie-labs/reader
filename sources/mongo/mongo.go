package mongo

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/writers"
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
	ctx := context.Background()
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
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

func (s *Source) Run(ctx context.Context, writer writers.Writer) error {
	if s.cfg.Streaming {
		iterator, err := newStreamingIterator(ctx, s.db, s.cfg, s.cfg.OffsetsFile)
		if err != nil {
			return err
		}

		if _, err = writer.Write(ctx, iterator); err != nil {
			return fmt.Errorf("failed to stream: %w", err)
		}
	} else {
		for _, collection := range s.cfg.Collections {
			snapshotStartTime := time.Now()

			slog.Info("Scanning collection",
				slog.String("collectionName", collection.Name),
				slog.String("topicSuffix", collection.TopicSuffix(s.db.Name())),
				slog.Any("batchSize", collection.GetBatchSize()),
			)

			iterator := newSnapshotIterator(s.db, collection, s.cfg)
			count, err := writer.Write(ctx, iterator)
			if err != nil {
				return fmt.Errorf("failed to snapshot collection %q: %w", collection.Name, err)
			}

			slog.Info("Finished snapshotting", slog.Int("scannedTotal", count), slog.Duration("totalDuration", time.Since(snapshotStartTime)))
		}
	}

	return nil
}
