package mongo

import (
	"context"
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type collectionScanner struct {
	db         *mongo.Database
	cfg        config.MongoDB
	collection config.Collection

	// mutable
	cursor *mongo.Cursor
	done   bool
}

func newIterator(db *mongo.Database, collection config.Collection, cfg config.MongoDB) *collectionScanner {
	return &collectionScanner{
		db:         db,
		cfg:        cfg,
		collection: collection,
	}
}

func (c *collectionScanner) HasNext() bool {
	return !c.done
}

func (c *collectionScanner) Next() ([]lib.RawMessage, error) {
	if !c.HasNext() {
		return nil, fmt.Errorf("no more rows to scan")
	}

	ctx := context.Background()
	if c.cursor == nil {
		findOptions := options.Find()
		findOptions.SetBatchSize(c.collection.GetBatchSize())
		cursor, err := c.db.Collection(c.collection.Name).Find(ctx, bson.D{}, findOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to find documents: %w", err)
		}

		c.cursor = cursor
	}

	var mgoMsgs []mgoMessage
	for c.collection.GetBatchSize() > int32(len(mgoMsgs)) && c.cursor.Next(ctx) {
		var result bson.M
		if err := c.cursor.Decode(&result); err != nil {
			return nil, fmt.Errorf("failed to decode document: %w", err)
		}

		mgoMsg, err := parseMessage(result)
		if err != nil {
			return nil, fmt.Errorf("failed to parse message: %w", err)
		}

		mgoMsgs = append(mgoMsgs, *mgoMsg)
	}

	if err := c.cursor.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate over documents: %w", err)
	}

	// If the number of fetched documents is less than the batch size, we are done
	if c.collection.GetBatchSize() > int32(len(mgoMsgs)) {
		c.done = true
	}

	var rawMessages []lib.RawMessage
	for _, mgoMsg := range mgoMsgs {
		rawMessage, err := mgoMsg.toRawMessage(c.collection, c.cfg.Database)
		if err != nil {
			return nil, fmt.Errorf("failed to create raw message: %w", err)
		}

		rawMessages = append(rawMessages, rawMessage)
	}

	return rawMessages, nil
}
