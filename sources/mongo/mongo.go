package mongo

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/artie-labs/reader/config"
	mongoLib "github.com/artie-labs/reader/lib/mongo"
	"github.com/artie-labs/reader/writers"
)

type Source struct {
	cfg config.MongoDB
	db  *mongo.Database
}

func Load(ctx context.Context, cfg config.MongoDB) (*Source, bool, error) {
	opts, err := mongoLib.OptsFromConfig(cfg)
	if err != nil {
		return nil, false, fmt.Errorf("failed to build options for MongoDB: %w", err)
	}
	if err := opts.Validate(); err != nil {
		return nil, false, fmt.Errorf("validation failed for MongoDB options: %w", err)
	}

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, false, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, false, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	return &Source{cfg: cfg, db: client.Database(cfg.Database)}, cfg.StreamingSettings.Enabled, nil
}

func (s *Source) Close() error {
	// MongoDB doesn't need to be closed.
	return nil
}

func (s *Source) Run(ctx context.Context, writer writers.Writer) error {
	if s.cfg.StreamingSettings.Enabled {
		iterator, err := newStreamingIterator(ctx, s.db, s.cfg, s.cfg.StreamingSettings.OffsetFile)
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

			iterator := NewSnapshotIterator(s.db, collection, s.cfg)
			count, err := writer.Write(ctx, iterator)
			if err != nil {
				return fmt.Errorf("failed to snapshot collection %q: %w", collection.Name, err)
			}

			slog.Info("Finished snapshotting", slog.Int("scannedTotal", count), slog.Duration("totalDuration", time.Since(snapshotStartTime)))
		}
	}

	return nil
}
