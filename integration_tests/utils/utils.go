package utils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"

	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/lib/debezium/transformer"
	"github.com/artie-labs/reader/lib/kafkalib"
)

func CreateTemporaryTable(db *sql.DB, query string) (string, func()) {
	tempTableName := fmt.Sprintf("artie_reader_%d", 10_000+rand.Int32N(10_000))
	slog.Info("Creating temporary table...", slog.String("table", tempTableName))
	if _, err := db.Exec(fmt.Sprintf(query, tempTableName)); err != nil {
		panic(err)
	}
	return tempTableName, func() {
		slog.Info("Dropping temporary table...", slog.String("table", tempTableName))
		if _, err := db.Exec(fmt.Sprintf("DROP TABLE %s", tempTableName)); err != nil {
			slog.Error("Failed to drop table", slog.Any("err", err))
		}
	}
}

func ReadTable(dbzAdapter transformer.Adapter) ([]kafkalib.Message, error) {
	dbzTransformer, err := transformer.NewDebeziumTransformer(dbzAdapter)
	if err != nil {
		return nil, err
	}

	var rows []kafkalib.Message
	for dbzTransformer.HasNext() {
		batch, err := dbzTransformer.Next()
		if err != nil {
			return nil, err
		}
		rows = append(rows, batch...)
	}
	return rows, nil
}

func GetMongoEvent(message kafkalib.Message) mongo.SchemaEventPayload {
	event, ok := message.Event().(*mongo.SchemaEventPayload)
	if !ok {
		panic("event is not of type *mongo.SchemaEventPayload")
	}
	return *event
}

func GetEvent(message kafkalib.Message) util.SchemaEventPayload {
	event, ok := message.Event().(*util.SchemaEventPayload)
	if !ok {
		panic("event is not of type *util.SchemaEventPayload")
	}
	return *event
}

func CheckDifference(name, expected, actual string) bool {
	if expected == actual {
		return false
	}
	expectedLines := strings.Split(expected, "\n")
	actualLines := strings.Split(actual, "\n")
	fmt.Println("--------------------------------------------------------------------------------")
	for i := range max(len(expectedLines), len(actualLines)) {
		if i < len(expectedLines) {
			if i < len(actualLines) {
				if expectedLines[i] == actualLines[i] {
					fmt.Println(expectedLines[i])
				} else {
					fmt.Println("E" + expectedLines[i])
					fmt.Println("A" + actualLines[i])
				}
			} else {
				fmt.Println("E" + expectedLines[i])
			}
		} else {
			fmt.Println("A" + actualLines[i])
		}
	}
	fmt.Println("--------------------------------------------------------------------------------")
	return true
}

func CheckPartitionKeyDifference(expected, actual debezium.PrimaryKeyPayload) (bool, error) {
	expectedBytes, err := json.Marshal(expected)
	if err != nil {
		return false, fmt.Errorf("failed to marshal expected: %w", err)
	}

	actualBytes, err := json.Marshal(actual)
	if err != nil {
		return false, fmt.Errorf("failed to marshal actual: %w", err)
	}

	return string(expectedBytes) == string(actualBytes), nil
}
