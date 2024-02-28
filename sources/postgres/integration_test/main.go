package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/debezium"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/lib/postgres"
	"github.com/artie-labs/reader/sources/postgres/adapter"
	"github.com/artie-labs/transfer/lib/cdc/util"
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

func rawMessageTimestamp(message lib.RawMessage) int64 {
	payloadTyped, ok := message.GetPayload().(util.SchemaEventPayload)
	if !ok {
		panic("payload is not of type util.SchemaEventPayload")
	}
	return payloadTyped.Payload.Source.TsMs
}

func checkDifference(name, expected, actual string) bool {
	if expected == actual {
		return false
	}
	expectedLines := strings.Split(expected, "\n")
	actualLines := strings.Split(actual, "\n")
	fmt.Printf("Expected %s:\n", name)
	fmt.Println("--------------------------------------------------------------------------------")
	for i, line := range expectedLines {
		prefix := " "
		if i >= len(actualLines) || line != actualLines[i] {
			prefix = ">"
		}
		fmt.Println(prefix + line)
	}
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("Actual %s:\n", name)
	fmt.Println("--------------------------------------------------------------------------------")
	for i, line := range actualLines {
		prefix := " "
		if i >= len(expectedLines) || line != expectedLines[i] {
			prefix = ">"
		}
		fmt.Println(prefix + line)
	}
	fmt.Println("--------------------------------------------------------------------------------")
	return true
}

func readTable(db *sql.DB, tableName string) ([]lib.RawMessage, error) {
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
	return rows, nil
}

const createTableQuery = `
CREATE TABLE %s (
	pk integer PRIMARY KEY NOT NULL,
	-- All the types from https://www.postgresql.org/docs/current/datatype.html#DATATYPE-TABLE
	c_bigint bigint,
	c_bigserial bigserial,
	c_bit bit,
	c_boolean boolean,
	-- c_box box,
	-- c_bytea bytea,
	c_character character,
	c_character_varying character varying,
	c_cidr cidr,
	-- c_circle circle,
	c_date date,
	c_double_precision double precision,
	c_inet inet,
	c_integer integer,
	c_interval interval,
	c_json json,
	c_jsonb jsonb,
	-- c_line line,
	-- c_lseg lseg,
	c_macaddr macaddr,
	c_macaddr8 macaddr8,
	c_money money,
	c_numeric numeric(7, 2),
	-- c_path path,
	-- c_pg_lsn pg_lsn,
	-- c_pg_snapshot pg_snapshot,
	c_point point,
	-- c_polygon polygon,
	c_real real,
	c_smallint smallserial,
	c_serial serial,
	c_text text,
	c_time_without_timezone time WITHOUT TIME ZONE,
	c_time_with_timezone time WITH TIME ZONE,
	c_timestamp_without_timezone timestamp WITHOUT TIME ZONE,
	c_timestamp_with_timezone timestamp WITH TIME ZONE,
	-- c_tsquery tsquery,
	-- c_tsvector tsvector,
	-- c_txid_snapshot txid_snapshot,
	c_uuid uuid,
	c_xml xml,
	-- User defined
	c_hstore hstore,
	c_geometry geometry,
	c_geography geography(Point)
)
`

const insertQuery = `
INSERT INTO %s VALUES (
	-- pk
		1,
	-- c_bigint
		9009900990099009000,
	-- c_bigserial
		100000123100000123,
	-- c_bit
		B'1',
	-- c_boolean
		true,
	-- c_box
		-- Not supported
	-- c_bytea
		-- Not supported
	-- c_character
		'X',
	-- c_character_varying
		'ASDFGHJKL',
	-- c_cidr
		'192.168.100.128/25',
	-- c_circle
		-- Not supported
	-- c_date
		'2020-01-02',
	-- c_double_precision
		123.456,
	-- c_inet
		'192.168.1.5',
	-- c_integer
		12345,
	-- c_interval
		'2 hour',
	-- c_json
		'{"foo": "bar", "baz": 1234}',
	-- c_jsonb
		'{"foo": "bar", "baz": 4321}'::jsonb,
	-- c_line
		-- Not supported
	-- c_lseg
		-- Not supported
	-- c_macaddr
		'12:34:56:78:90:ab',
	-- c_macaddr8
		'12:34:56:78:90:ab:cd:ef',
	-- c_money
		'52093.89',
	-- c_numeric
		'987.654',
	-- c_path
		-- Not supported
	-- c_pg_lsn
		-- Not supported
	-- c_pg_snapshot
		-- Not supported
	-- c_point
		POINT(12.34, 56.78),
	-- c_polygon
		-- Not supported
	-- c_real
		45.678,
	-- c_smallint
		32767,
	-- c_serial
		1000000123,
	-- c_text
		'QWERTYUIOP',
	-- c_time_without_timezone
		'12:34:56',
	-- c_time_with_timezone
		time with time zone '05:34:17-05',
	-- c_timestamp_without_timezone
		'2001-02-16 20:38:40',
	-- c_timestamp_with_timezone
		'2001-02-16 20:38:40' AT TIME ZONE 'America/Denver',
	-- c_tsquery
		-- Not supported
	-- c_tsvector
		-- Not supported
	-- c_txid_snapshot
		-- Not supported
	-- c_uuid
		'e7082e96-7190-4cc3-8ab4-bd27f1269f08',
	-- c_xml
		'<html><head>Hello</head><body>World</body></html>',
	-- c_hstore
		'"a" => "b", "c" => "d", "e" => "f"',
	-- c_geometry
		'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))',
	-- c_geography
		'POINT(-118.4079 33.9434)'
)
`

const expectedPartitionKey = `{
"pk": 1
}`

const expectedPayloadTemplate = `{
	"schema": {
		"type": "",
		"fields": [
			{
				"type": "",
				"fields": [
					{
						"type": "int32",
						"optional": false,
						"default": null,
						"field": "pk",
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
						"field": "c_bigserial",
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
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_character",
						"name": "",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_character_varying",
						"name": "",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_cidr",
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
						"type": "float",
						"optional": false,
						"default": null,
						"field": "c_double_precision",
						"name": "",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_inet",
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
						"field": "c_interval",
						"name": "io.debezium.time.MicroDuration",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_json",
						"name": "io.debezium.data.Json",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_jsonb",
						"name": "io.debezium.data.Json",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_macaddr",
						"name": "",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_macaddr8",
						"name": "",
						"parameters": null
					},
					{
						"type": "",
						"optional": false,
						"default": null,
						"field": "c_money",
						"name": "org.apache.kafka.connect.data.Decimal",
						"parameters": {
							"scale": "2"
						}
					},
					{
						"type": "",
						"optional": false,
						"default": null,
						"field": "c_numeric",
						"name": "org.apache.kafka.connect.data.Decimal",
						"parameters": {
							"connect.decimal.precision": "7",
							"scale": "2"
						}
					},
					{
						"type": "struct",
						"optional": false,
						"default": null,
						"field": "c_point",
						"name": "io.debezium.data.geometry.Point",
						"parameters": null
					},
					{
						"type": "float",
						"optional": false,
						"default": null,
						"field": "c_real",
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
						"field": "c_serial",
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
					},
					{
						"type": "int32",
						"optional": false,
						"default": null,
						"field": "c_time_without_timezone",
						"name": "io.debezium.time.Time",
						"parameters": null
					},
					{
						"type": "int32",
						"optional": false,
						"default": null,
						"field": "c_time_with_timezone",
						"name": "io.debezium.time.Time",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_timestamp_without_timezone",
						"name": "io.debezium.time.Timestamp",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_timestamp_with_timezone",
						"name": "io.debezium.time.Timestamp",
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
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_xml",
						"name": "",
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
						"type": "struct",
						"optional": false,
						"default": null,
						"field": "c_geometry",
						"name": "io.debezium.data.geometry.Geometry",
						"parameters": null
					},
					{
						"type": "struct",
						"optional": false,
						"default": null,
						"field": "c_geography",
						"name": "io.debezium.data.geometry.Geography",
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
			"c_bigint": 9009900990099009000,
			"c_bigserial": 100000123100000123,
			"c_bit": true,
			"c_boolean": true,
			"c_character": "X",
			"c_character_varying": "ASDFGHJKL",
			"c_cidr": "192.168.100.128/25",
			"c_date": 18263,
			"c_double_precision": 123.456,
			"c_geography": {
				"srid": null,
				"wkb": "AQEAACDmEAAAdQKaCBuaXcDwhclUwfhAQA=="
			},
			"c_geometry": {
				"srid": null,
				"wkb": "AQMAAAABAAAABQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADwPwAAAAAAAAAAAAAAAAAA8D8AAAAAAADwPwAAAAAAAAAAAAAAAAAA8D8AAAAAAAAAAAAAAAAAAAAA"
			},
			"c_hstore": {
				"a": "b",
				"c": "d",
				"e": "f"
			},
			"c_inet": "192.168.1.5/32",
			"c_integer": 12345,
			"c_interval": 7200000000,
			"c_json": "{\"foo\": \"bar\", \"baz\": 1234}",
			"c_jsonb": "{\"baz\": 4321, \"foo\": \"bar\"}",
			"c_macaddr": "12:34:56:78:90:ab",
			"c_macaddr8": "12:34:56:78:90:ab:cd:ef",
			"c_money": "T30t",
			"c_numeric": "AYHN",
			"c_point": {
				"x": 12.34,
				"y": 56.78
			},
			"c_real": 45.678001403808594,
			"c_serial": 1000000123,
			"c_smallint": 32767,
			"c_text": "QWERTYUIOP",
			"c_time_with_timezone": 38057000,
			"c_time_without_timezone": 45296000,
			"c_timestamp_with_timezone": "2001-02-16T05:38:40-08:00",
			"c_timestamp_without_timezone": "2001-02-16T20:38:40Z",
			"c_uuid": "e7082e96-7190-4cc3-8ab4-bd27f1269f08",
			"c_xml": "\u003chtml\u003e\u003chead\u003eHello\u003c/head\u003e\u003cbody\u003eWorld\u003c/body\u003e\u003c/html\u003e",
			"pk": 1
		},
		"source": {
			"connector": "",
			"ts_ms": %d,
			"db": "",
			"schema": "",
			"table": "%s"
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
	defer func() {
		slog.Info("Dropping temporary table...", slog.String("table", tempTableName))
		if _, err := db.Exec(fmt.Sprintf("DROP TABLE %s", tempTableName)); err != nil {
			slog.Error("Failed to drop table", slog.Any("err", err))
		}
	}()

	slog.Info("Inserting data...")
	_, err = db.Exec(fmt.Sprintf(insertQuery, tempTableName))
	if err != nil {
		return fmt.Errorf("unable to insert data: %w", err)
	}

	rows, err := readTable(db, tempTableName)
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

	expectedPayload := fmt.Sprintf(expectedPayloadTemplate, rawMessageTimestamp(row), tempTableName)
	if checkDifference("payload", expectedPayload, string(valueBytes)) {
		return fmt.Errorf("payload does not match")
	}

	return nil
}
