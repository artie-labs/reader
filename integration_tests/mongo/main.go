package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"reflect"
	"time"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/integration_tests/utils"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/logger"
	mongoLib "github.com/artie-labs/reader/sources/mongo"
	xferMongo "github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	if err := os.Setenv("TZ", "UTC"); err != nil {
		logger.Fatal("Unable to set TZ env var: %w", err)
	}

	var mongoHost = cmp.Or(os.Getenv("MONGO_HOST"), "localhost")
	mongoCfg := config.MongoDB{
		URI:      fmt.Sprintf("mongodb://root:example@%s:27017", mongoHost),
		Database: "test",
	}

	// Not using TLS
	opts := options.Client().ApplyURI(mongoCfg.URI)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancel()

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		logger.Fatal("Could not connect to MongoDB", slog.Any("err", err))
	}

	db := client.Database(mongoCfg.Database)
	if err = testTypes(ctx, db, mongoCfg); err != nil {
		logger.Fatal("Types test failed", slog.Any("err", err))
	}

	slog.Info("✅ Test succeeded")
}

func readTable(db *mongo.Database, collection config.Collection, cfg config.MongoDB) ([]lib.RawMessage, error) {
	var totalMessages []lib.RawMessage
	iter := mongoLib.NewSnapshotIterator(db, collection, cfg)
	for iter.HasNext() {
		msgs, err := iter.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to read message: %w", err)
		}

		totalMessages = append(totalMessages, msgs...)
	}

	return totalMessages, nil
}

// testTypes checks that MongoDB data types are handled correctly.
func testTypes(ctx context.Context, db *mongo.Database, mongoCfg config.MongoDB) error {
	tempTableName := fmt.Sprintf("artie_reader_%d", 10_000+rand.Int32N(10_000))

	collection := db.Collection(tempTableName)

	defer func() {
		_ = collection.Drop(ctx)
	}()

	slog.Info("Inserting data...")

	objId, err := primitive.ObjectIDFromHex("66a95fae3776c2f21f0ff568")
	if err != nil {
		return fmt.Errorf("failed to parse object ID: %w", err)
	}

	ts := time.Date(2020, 10, 5, 12, 0, 0, 0, time.UTC)

	_, err = collection.InsertOne(ctx, bson.D{
		{Key: "_id", Value: objId},
		{Key: "string", Value: "This is a string"},
		{Key: "int32", Value: int32(32)},
		{Key: "int64", Value: int64(64)},
		{Key: "double", Value: 3.14},
		{Key: "bool", Value: true},
		{Key: "datetime", Value: ts},
		{Key: "embeddedDocument", Value: bson.D{
			{Key: "field1", Value: "value1"},
			{Key: "field2", Value: "value2"},
		}},
		{Key: "embeddedMap", Value: bson.M{"foo": "bar", "hello": "world", "pi": 3.14159}},
		{Key: "array", Value: bson.A{"item1", 2, true, 3.14}},
		{Key: "binary", Value: []byte("binary data")},
		{Key: "objectId", Value: objId},
		{Key: "null", Value: nil},
		{Key: "timestamp", Value: primitive.Timestamp{T: uint32(ts.Unix()), I: 1}},
		{Key: "minKey", Value: primitive.MinKey{}},
		{Key: "maxKey", Value: primitive.MaxKey{}},
	})
	if err != nil {
		return fmt.Errorf("failed to insert row: %w", err)
	}

	rows, err := readTable(db, config.Collection{Name: tempTableName}, mongoCfg)
	if err != nil {
		return err
	}

	if len(rows) != 1 {
		return fmt.Errorf("expected one row, got %d", len(rows))
	}

	row := rows[0]
	expectedPartitionKey := map[string]any{"payload": map[string]any{"id": `{"$oid":"66a95fae3776c2f21f0ff568"}`}}
	expectedPkBytes, err := json.Marshal(expectedPartitionKey)
	if err != nil {
		return fmt.Errorf("failed to marshal expected partition key: %w", err)
	}

	actualPkBytes, err := json.Marshal(row.PartitionKey())
	if err != nil {
		return fmt.Errorf("failed to marshal actual partition key: %w", err)
	}

	if string(expectedPkBytes) != string(actualPkBytes) {
		return fmt.Errorf("partition key %s does not match %s", actualPkBytes, expectedPkBytes)
	}

	mongoEvt := utils.GetMongoEvent(row)
	if mongoEvt.GetTableName() != tempTableName {
		return fmt.Errorf("table name does not match")
	}

	if mongoEvt.Payload.Source.Collection != tempTableName {
		return fmt.Errorf("collection does not match")
	}

	if mongoEvt.Payload.Operation != "r" {
		return fmt.Errorf("operation does not match")
	}

	var dbz xferMongo.Debezium
	valueBytes, err := json.Marshal(row.Event())
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	evt, err := dbz.GetEventFromBytes(valueBytes)
	if err != nil {
		return fmt.Errorf("failed to get event from bytes: %w", err)
	}

	pkMap, err := dbz.GetPrimaryKey(actualPkBytes, kafkalib.TopicConfig{CDCKeyFormat: kafkalib.JSONKeyFmt})
	if err != nil {
		return fmt.Errorf("failed to get primary key: %w", err)
	}

	data, err := evt.GetData(pkMap, kafkalib.TopicConfig{})
	if err != nil {
		return fmt.Errorf("failed to get data: %w", err)
	}

	expectedPayload := map[string]any{
		"objectId":                "66a95fae3776c2f21f0ff568",
		"array":                   []any{"item1", int32(2), true, 3.14},
		"datetime":                ext.NewExtendedTime(ts, ext.TimestampTzKindType, "2006-01-02T15:04:05.999-07:00"),
		"int64":                   int64(64),
		"__artie_delete":          false,
		"__artie_only_set_delete": false,
		"timestamp":               ext.NewExtendedTime(ts, ext.TimestampTzKindType, "2006-01-02T15:04:05.999-07:00"),
		"embeddedDocument":        `{"field1":"value1","field2":"value2"}`,
		"embeddedMap":             `{"foo":"bar","hello":"world","pi":3.14159}`,
		"binary":                  `{"$binary":{"base64":"YmluYXJ5IGRhdGE=","subType":"00"}}`,
		"maxKey":                  `{"$maxKey":1}`,
		"minKey":                  `{"$minKey":1}`,
		"_id":                     "66a95fae3776c2f21f0ff568",
		"bool":                    true,
		"double":                  3.14,
		"string":                  "This is a string",
		"int32":                   int32(32),
		"null":                    nil,
	}

	var diffs []string
	for expectedKey, expectedValue := range expectedPayload {
		actualValue, isOk := data[expectedKey]
		delete(data, expectedKey)
		if !isOk {
			diffs = append(diffs, fmt.Sprintf("expected key %s not found", expectedKey))
			continue
		}

		if reflect.DeepEqual(expectedValue, actualValue) {
			continue
		}

		diffs = append(diffs, fmt.Sprintf("key: %s's expected value (%v), type: %T does not match actual value (%v), type: %T", expectedKey, expectedValue, expectedValue, actualValue, actualValue))
	}

	for actualKey, actualValue := range data {
		diffs = append(diffs, fmt.Sprintf("unexpected key %s with value %v", actualKey, actualValue))
	}

	if len(diffs) > 0 {
		for _, diff := range diffs {
			fmt.Println("diff", diff)
		}

		return fmt.Errorf("data does not match expected payload")
	}

	return nil
}
