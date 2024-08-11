package mongo

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/iterator"
	"github.com/artie-labs/reader/lib/persistedmap"
)

const offsetKey = "offset"

type streaming struct {
	db                    *mongo.Database
	cfg                   config.MongoDB
	changeStream          *mongo.ChangeStream
	ctx                   context.Context
	collectionsToWatchMap map[string]config.Collection
	offsets               *persistedmap.PersistedMap
	batchSize             int32
}

func newStreamingIterator(ctx context.Context, db *mongo.Database, cfg config.MongoDB, filePath string) (iterator.StreamingIterator[[]lib.RawMessage], error) {
	collectionsToWatchMap := make(map[string]config.Collection)
	for _, collection := range cfg.Collections {
		collectionsToWatchMap[collection.Name] = collection
	}

	// We only care about DMLs, the full list can be found here: https://www.mongodb.com/docs/manual/reference/change-events/
	pipeline := mongo.Pipeline{
		{{"$match", bson.D{
			{"operationType", bson.D{
				{"$in", bson.A{"insert", "update", "delete", "replace"}},
			}},
		}}},
	}

	opts := options.ChangeStream().
		// Setting `updateLookup` will emit the whole document for updates
		// Ref: https://www.mongodb.com/docs/manual/reference/change-events/update/#description
		SetFullDocument(options.UpdateLookup).
		// FullDocumentBeforeChange will kick in if the db + collection enabled `changeStreamPreAndPostImages`
		SetFullDocumentBeforeChange(options.WhenAvailable)

	storage := persistedmap.NewPersistedMap(filePath)
	if encodedResumeToken, exists := storage.Get(offsetKey); exists {
		castedEncodedResumeToken, isOk := encodedResumeToken.(string)
		if !isOk {
			return nil, fmt.Errorf("expected resume token to be string, got: %T", encodedResumeToken)
		}

		decodedBytes, err := base64.StdEncoding.DecodeString(castedEncodedResumeToken)
		if err != nil {
			return nil, fmt.Errorf("failed to decode resume token: %w", err)
		}

		var token bson.Raw
		if err = bson.Unmarshal(decodedBytes, &token); err != nil {
			return nil, fmt.Errorf("failed to unmarshal resume token: %w", err)
		}

		opts.SetResumeAfter(token)
	}

	cs, err := db.Watch(ctx, pipeline, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to start change stream: %w", err)
	}

	return &streaming{
		batchSize:             cfg.GetStreamingBatchSize(),
		db:                    db,
		cfg:                   cfg,
		changeStream:          cs,
		ctx:                   ctx,
		collectionsToWatchMap: collectionsToWatchMap,
		offsets:               storage,
	}, nil
}

func (s *streaming) HasNext() bool {
	return true
}

func (s *streaming) CommitOffset() {
	offset := base64.StdEncoding.EncodeToString(s.changeStream.ResumeToken())
	slog.Info("Committing offset", slog.String("offset", offset))
	s.offsets.Set(offsetKey, offset)
}

func (s *streaming) Next() ([]lib.RawMessage, error) {
	var rawMsgs []lib.RawMessage
	for s.batchSize > int32(len(rawMsgs)) && s.changeStream.TryNext(s.ctx) {
		var rawChangeEvent bson.M
		if err := s.changeStream.Decode(&rawChangeEvent); err != nil {
			return nil, fmt.Errorf("failed to decode change event: %w", err)
		}

		changeEvent, err := NewChangeEvent(rawChangeEvent)
		if err != nil {
			return nil, fmt.Errorf("failed to parse change event: %w", err)
		}

		collection, watching := s.collectionsToWatchMap[changeEvent.Collection()]
		if !watching {
			continue
		}

		msg, err := changeEvent.ToMessage()
		if err != nil {
			return nil, fmt.Errorf("failed to get message: %w", err)
		}

		rawMessage, err := msg.ToRawMessage(collection, s.cfg.Database)
		if err != nil {
			return nil, fmt.Errorf("failed to convert message to raw message: %w", err)
		}

		rawMsgs = append(rawMsgs, rawMessage)
	}

	if len(rawMsgs) == 0 {
		// If there are no messages, let's sleep a bit before we try again
		time.Sleep(2 * time.Second)
	}

	return rawMsgs, nil
}
