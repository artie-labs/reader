package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/debezium"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/lib/postgres"
	"github.com/artie-labs/reader/sources/postgres/adapter"
	"github.com/artie-labs/transfer/lib/cdc/util"
)

const (
	replacedTimestamp = 1700000000000
	replacedTableName = "__replaced_table_name__"
)

var pgConfig = config.PostgreSQL{
	Host:     "127.0.0.1",
	Port:     5432,
	Username: "postgres",
	Password: "postgres",
	Database: "postgres",
}

func main() {
	db, err := sql.Open("pgx", pgConfig.ToDSN())
	if err != nil {
		logger.Fatal("Could not connect to Postgres", slog.Any("err", err))
	}

	err = testTypes(db)
	if err != nil {
		logger.Fatal("Types test failed", slog.Any("err", err))
	}
}

// cleanRawMessages override `lib.RawMessage` attributes which vary per-run
func cleanRawMessages(tableName string, rows []lib.RawMessage) error {
	for i, row := range rows {
		payload := row.GetPayload()
		payloadTyped, ok := payload.(util.SchemaEventPayload)
		if !ok {
			return fmt.Errorf("payload is not of type util.SchemaEventPayload")
		}
		// Override source timestamp since it will always be the current time
		payloadTyped.Payload.Source.TsMs = replacedTimestamp
		if payloadTyped.Payload.Source.Table != tableName {
			return fmt.Errorf("payload source table name is not the table name")
		}
		// Override source table since it will be random each time
		payloadTyped.Payload.Source.Table = replacedTableName
		rows[i] = lib.NewRawMessage(row.TopicSuffix, row.PartitionKey, payloadTyped)
	}
	return nil
}

func checkDifference(name, expected, actual string) bool {
	if expected != actual {
		fmt.Printf("Expected %s:\n", name)
		fmt.Println("--------------------------------------------------------------------------------")
		fmt.Println(expectedPartitionKey)
		fmt.Println("--------------------------------------------------------------------------------")
		fmt.Printf("Actual %s:\n", name)
		fmt.Println("--------------------------------------------------------------------------------")
		fmt.Println(actual)
		fmt.Println("--------------------------------------------------------------------------------")
		return true
	}
	return false
}

func read(db *sql.DB, tableName string) ([]lib.RawMessage, error) {
	tableCfg := config.PostgreSQLTable{
		Schema: "public",
		Name:   tableName,
	}

	table := postgres.NewTable(tableCfg.Schema, tableCfg.Name)
	if err := table.PopulateColumns(db); err != nil {
		return nil, fmt.Errorf("unable to load table metadata: %w", err)
	}

	scanner, err := table.NewScanner(db, tableCfg.ToScannerConfig(1))
	if err != nil {
		return nil, fmt.Errorf("failed to build scanner: %w", err)
	}
	dbzTransformer := debezium.NewDebeziumTransformer(adapter.NewPostgresAdapter(*table), &scanner)
	rows := []lib.RawMessage{}
	for dbzTransformer.HasNext() {
		batch, err := dbzTransformer.Next()
		if err != nil {
			logger.Fatal("Failed to get batch", slog.Any("err", err))
		}
		rows = append(rows, batch...)
	}

	if err = cleanRawMessages(tableName, rows); err != nil {
		return nil, err
	}

	return rows, nil
}

const createTableQuery = `
CREATE TABLE %s (
	c_real real,
	c_double_precision double precision,
	c_smallint smallint,
	c_integer integer primary key,
	c_bigint bigint,
	c_oid oid,
	c_int_array integer[],
	c_text_array text[],
	c_bit bit,
	c_boolean boolean,
	c_date date,
	c_uuid uuid,
	c_hstore hstore,
	c_text text
)
`

const insertQuery = `
INSERT INTO %s VALUES (
	12.34,
	34.56,
	12,
	34,
	56,
	78,
	'{1,2,3}',
	'{"aa","bb","cc"}',
	B'1',
	true,
	'2020-01-02',
	'e7082e96-7190-4cc3-8ab4-bd27f1269f08',
	'"a" => "b", "c" => "d", "e" => "f"',
	'foo bar'
)
`

const expectedPartitionKey = `{
"c_integer": 34
}`
const expectedPayload = `{
	"schema": {
		"type": "",
		"fields": [
			{
				"type": "",
				"fields": [
					{
						"type": "float",
						"optional": false,
						"default": null,
						"field": "c_real",
						"name": "",
						"parameters": null
					},
					{
						"type": "float",
						"optional": false,
						"default": null,
						"field": "c_double_precision",
						"name": "",
						"parameters": null
					},
					{
						"type": "int16",
						"optional": false,
						"default": null,
						"field": "c_smallint",
						"name": "",
						"parameters": null
					},
					{
						"type": "int32",
						"optional": false,
						"default": null,
						"field": "c_integer",
						"name": "",
						"parameters": null
					},
					{
						"type": "int64",
						"optional": false,
						"default": null,
						"field": "c_bigint",
						"name": "",
						"parameters": null
					},
					{
						"type": "int64",
						"optional": false,
						"default": null,
						"field": "c_oid",
						"name": "",
						"parameters": null
					},
					{
						"type": "array",
						"optional": false,
						"default": null,
						"field": "c_int_array",
						"name": "",
						"parameters": null
					},
					{
						"type": "array",
						"optional": false,
						"default": null,
						"field": "c_text_array",
						"name": "",
						"parameters": null
					},
					{
						"type": "boolean",
						"optional": false,
						"default": null,
						"field": "c_bit",
						"name": "",
						"parameters": null
					},
					{
						"type": "boolean",
						"optional": false,
						"default": null,
						"field": "c_boolean",
						"name": "",
						"parameters": null
					},
					{
						"type": "int32",
						"optional": false,
						"default": null,
						"field": "c_date",
						"name": "io.debezium.time.Date",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_uuid",
						"name": "io.debezium.data.Uuid",
						"parameters": null
					},
					{
						"type": "map",
						"optional": false,
						"default": null,
						"field": "c_hstore",
						"name": "",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_text",
						"name": "",
						"parameters": null
					}
				],
				"optional": false,
				"field": "after"
			}
		]
	},
	"payload": {
		"before": null,
		"after": {
			"c_bigint": 56,
			"c_bit": true,
			"c_boolean": true,
			"c_date": 18263,
			"c_double_precision": 34.56,
			"c_hstore": {
				"a": "b",
				"c": "d",
				"e": "f"
			},
			"c_int_array": [
				1,
				2,
				3
			],
			"c_integer": 34,
			"c_oid": 78,
			"c_real": 12.34000015258789,
			"c_smallint": 12,
			"c_text": "foo bar",
			"c_text_array": [
				"aa",
				"bb",
				"cc"
			],
			"c_uuid": "e7082e96-7190-4cc3-8ab4-bd27f1269f08"
		},
		"source": {
			"connector": "",
			"ts_ms": 1700000000000,
			"db": "",
			"schema": "",
			"table": "__replaced_table_name__"
		},
		"op": "r"
	}
}`

func testTypes(db *sql.DB) error {
	tempTableName := fmt.Sprintf("artie_reader_%d", 10_000+rand.Int32N(5_000))
	slog.Info("Creating temporary table...", slog.String("table", tempTableName))
	_, err := db.Exec(fmt.Sprintf(createTableQuery, tempTableName))
	if err != nil {
		return fmt.Errorf("unable to create temporary table: %w", err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE %s", tempTableName))

	slog.Info("Inserting data...")
	_, err = db.Exec(fmt.Sprintf(insertQuery, tempTableName))
	if err != nil {
		return fmt.Errorf("unable to insert data: %w", err)
	}

	rows, err := read(db, tempTableName)
	if err != nil {
		return err
	}

	if len(rows) != 1 {
		return fmt.Errorf("expected one row, got %d", len(rows))
	}
	row := rows[0]

	keyBytes, err := json.MarshalIndent(row.PartitionKey, "", "")
	if err != nil {
		return fmt.Errorf("failed to marshal partition key: %w", err)
	}

	valueBytes, err := json.MarshalIndent(row.GetPayload(), "", "\t")
	if err != nil {
		return fmt.Errorf("failed to marshal payload")
	}

	if checkDifference("partition key", expectedPartitionKey, string(keyBytes)) {
		return fmt.Errorf("partition key does not match")
	}

	if checkDifference("payload", expectedPayload, string(valueBytes)) {
		return fmt.Errorf("payload does not match")
	}

	return nil
}
