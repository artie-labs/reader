package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type collectionScanner struct {
	db         *mongo.Database
	collection config.Collection

	// mutable
	cursor *mongo.Cursor
	done   bool
}

func newIterator(db *mongo.Database, collection config.Collection) *collectionScanner {
	return &collectionScanner{
		db:         db,
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
		findOptions.SetLimit(int64(c.collection.GetBatchSize()))

		cursor, err := c.db.Collection(c.collection.Name).Find(ctx, bson.D{}, findOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to find documents, err: %w", err)
		}

		c.cursor = cursor
	}

	var messages []map[string]interface{}
	for c.collection.GetBatchSize() > uint(len(messages)) && c.cursor.Next(ctx) {
		var result bson.M
		if err := c.cursor.Decode(&result); err != nil {
			return nil, fmt.Errorf("failed to decode document, err: %w", err)
		}

		jsonExtendedBytes, err := bson.MarshalExtJSON(result, false, false)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal document to JSON extended, err: %w", err)
		}

		var jsonExtendedMap map[string]interface{}
		if err = json.Unmarshal(jsonExtendedBytes, &jsonExtendedMap); err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON extended to map, err: %w", err)
		}

		messages = append(messages, jsonExtendedMap)
	}

	if err := c.cursor.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate over documents, err: %w", err)
	}

	// If the number of fetched documents is less than the batch size, we are done
	if c.collection.GetBatchSize() > uint(len(messages)) {
		c.done = true
	}

	var rawMessages []lib.RawMessage
	for _, message := range messages {
		rawMessage, err := newRawMessage(message, c.collection)
		if err != nil {
			return nil, fmt.Errorf("failed to create raw message, err: %w", err)
		}

		rawMessages = append(rawMessages, rawMessage)
	}

	return rawMessages, nil
}
