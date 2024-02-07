package mongo

import (
	"context"
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/mtr"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func snapshotCollection(ctx context.Context, db *mongo.Database, collection config.Collection, statsD *mtr.Client) error {
	findOptions := options.Find()
	findOptions.SetLimit(int64(collection.GetBatchSize()))

	cursor, err := db.Collection(collection.Name).Find(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("failed to find documents, err: %w", err)
	}

	defer func() {
		_ = cursor.Close(ctx)
	}()

	for cursor.Next(ctx) {
		var result bson.M
		if err = cursor.Decode(&result); err != nil {
			return fmt.Errorf("failed to decode document, err: %w", err)
		}

		fmt.Println("result", result)
	}

	if err = cursor.Err(); err != nil {
		return fmt.Errorf("failed to iterate over documents, err: %w", err)
	}

	return nil
}
