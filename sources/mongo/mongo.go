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

	return nil
}
