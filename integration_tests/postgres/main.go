package main

import (
	"cmp"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lmittmann/tint"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/integration_tests/utils"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/lib/rdbms"
	"github.com/artie-labs/reader/sources/postgres/adapter"
)

func main() {
	if err := os.Setenv("TZ", "UTC"); err != nil {
		logger.Fatal("Unable to set TZ env var: %w", err)
	}
	slog.SetDefault(slog.New(tint.NewHandler(os.Stderr, &tint.Options{})))

	pgConfig := config.PostgreSQL{
		Host:     cmp.Or(os.Getenv("PG_HOST"), "localhost"),
		Port:     5432,
		Username: "postgres",
		Password: "postgres",
		Database: "postgres",
	}

	db, err := sql.Open("pgx", pgConfig.ToDSN())
	if err != nil {
		logger.Fatal("Could not connect to Postgres", slog.Any("err", err))
	}

	if err = testTypes(db); err != nil {
		logger.Fatal("Types test failed", slog.Any("err", err))
	}

	if err = testScan(db); err != nil {
		logger.Fatal("Scan test failed", slog.Any("err", err))
	}

	if err = testPrimaryKeyTypes(db); err != nil {
		logger.Fatal("Primary key types test failed", slog.Any("err", err))
	}
}

func readTable(db *sql.DB, tableName string, batchSize int) ([]lib.RawMessage, error) {
	tableCfg := config.PostgreSQLTable{
		Schema:    "public",
		Name:      tableName,
		BatchSize: uint(batchSize),
	}

	dbzAdapter, err := adapter.NewPostgresAdapter(db, tableCfg)
	if err != nil {
		return nil, err
	}

	return utils.ReadTable(dbzAdapter)
}

const testTypesCreateTableQuery = `
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE TABLE %s (
	pk integer PRIMARY KEY NOT NULL,
	-- All the types from https://www.postgresql.org/docs/current/datatype.html#DATATYPE-TABLE
	c_bigint bigint,
	c_bigserial bigserial,
	c_bit bit,
	c_bit1 bit(1),
	c_bit5 bit(5),
	c_bit_varying bit varying,
	c_bit_varying5 bit varying(5),
	c_bit_varying10 bit varying(10),
	c_boolean boolean,
	-- c_box box,
	c_bytea bytea,
	c_character character,
	c_character_varying character varying,
	c_cidr cidr,
	-- c_circle circle,
	c_date date,
	c_double_precision double precision,
	c_inet_ipv4_no_subnet inet,
	c_inet_ipv4_subnet inet,
	c_inet_ipv6_no_subnet inet,
	c_inet_ipv6_subnet inet,
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
	c_numeric_nan numeric(7, 2),
	c_numeric_variable numeric,
	c_numeric_variable_nan numeric,
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
	-- Range types from https://www.postgresql.org/docs/current/rangetypes.html
	c_int4range int4range,
	c_int8range int8range,
	c_numrange numrange,
	c_tsrange tsrange,
	c_tstzrange tstzrange,
	c_daterange daterange,
	-- User defined
	c_hstore hstore,
	c_geometry geometry,
	c_geography geography(Point),
	-- Arrays
	c_int_array int[]
)
`

const testTypesInsertQuery = `
INSERT INTO %s VALUES (
	-- pk
		1,
	-- c_bigint
		9009900990099009000,
	-- c_bigserial
		100000123100000123,
	-- c_bit
		B'1',
	-- c_bit1
		B'1',
	-- c_bit5
		B'10101'
	-- c_bit_varying
		B'10101',
	-- c_bit_varying5
		B'10101',
	-- c_bit_varying10
		B'10101',
	-- c_boolean
		true,
	-- c_box
		-- Not supported
	-- c_bytea
		'abc \153\154\155 \052\251\124'::bytea,
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
	-- c_inet_ipv4_no_subnet
		'192.168.1.5',
	-- c_inet_ipv4_subnet
		'192.168.1.5/24',
	-- c_inet_ipv6_no_subnet
		'2001:4f8:3:ba:2e0:81ff:fe22:d1f1',
	-- c_inet_ipv6_subnet
		'2001:4f8:3:ba:2e0:81ff:fe22:d1f1/64',
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
	-- c_numeric_nan
		'NaN',
	-- c_numeric_variable,
		'10987.65401',
	-- c_numeric_variable_nan,
		'NaN',
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
		time with time zone '05:34:17.746572-05',
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
	-- c_int4range
		int4range(10, 20),
	-- c_int8range
		int8range(1009900990099009000, 9009900990099009000),
	-- c_numrange
		numrange(11.1, 22.2),
	-- c_tsrange
		'[2010-01-01 14:30, 2010-01-01 15:30)',
	-- c_tstzrange
		tstzrange('2001-02-16 20:38:40+12', '2001-03-20 20:38:40+12'),
	-- c_daterange
		'[2010-01-01, 2010-01-03]',
	-- c_hstore
		'"a" => "b", "c" => "d", "e" => "f"',
	-- c_geometry
		'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))',
	-- c_geography
		'POINT(-118.4079 33.9434)',
	-- c_int_array
		'{0,2,4,6}'
)
`

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
						"field": "c_bit1",
						"name": "",
						"parameters": null
					},
					{
						"type": "bytes",
						"optional": false,
						"default": null,
						"field": "c_bit5",
						"name": "io.debezium.data.Bits",
						"parameters": {
							"length": "5"
						}
					},
					{
						"type": "bytes",
						"optional": false,
						"default": null,
						"field": "c_bit_varying",
						"name": "io.debezium.data.Bits",
						"parameters": null
					},
					{
						"type": "bytes",
						"optional": false,
						"default": null,
						"field": "c_bit_varying5",
						"name": "io.debezium.data.Bits",
						"parameters": null
					},
					{
						"type": "bytes",
						"optional": false,
						"default": null,
						"field": "c_bit_varying10",
						"name": "io.debezium.data.Bits",
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
						"type": "bytes",
						"optional": false,
						"default": null,
						"field": "c_bytea",
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
						"type": "double",
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
						"field": "c_inet_ipv4_no_subnet",
						"name": "",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_inet_ipv4_subnet",
						"name": "",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_inet_ipv6_no_subnet",
						"name": "",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_inet_ipv6_subnet",
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
						"type": "bytes",
						"optional": false,
						"default": null,
						"field": "c_money",
						"name": "org.apache.kafka.connect.data.Decimal",
						"parameters": {
							"scale": "2"
						}
					},
					{
						"type": "bytes",
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
						"type": "bytes",
						"optional": false,
						"default": null,
						"field": "c_numeric_nan",
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
						"field": "c_numeric_variable",
						"name": "io.debezium.data.VariableScaleDecimal",
						"parameters": null
					},
					{
						"type": "struct",
						"optional": false,
						"default": null,
						"field": "c_numeric_variable_nan",
						"name": "io.debezium.data.VariableScaleDecimal",
						"parameters": null
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
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_time_with_timezone",
						"name": "io.debezium.time.ZonedTime",
						"parameters": null
					},
					{
						"type": "int64",
						"optional": false,
						"default": null,
						"field": "c_timestamp_without_timezone",
						"name": "io.debezium.time.MicroTimestamp",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_timestamp_with_timezone",
						"name": "io.debezium.time.ZonedTimestamp",
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
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_int4range",
						"name": "",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_int8range",
						"name": "",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_numrange",
						"name": "",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_tsrange",
						"name": "",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_tstzrange",
						"name": "",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_daterange",
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
					},
					{
						"type": "array",
						"optional": false,
						"default": null,
						"field": "c_int_array",
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
			"c_bigint": 9009900990099009000,
			"c_bigserial": 100000123100000123,
			"c_bit": true,
			"c_bit1": true,
			"c_bit5": "FQ==",
			"c_boolean": true,
			"c_bytea": "YWJjIGtsbSAqqVQ=",
			"c_character": "X",
			"c_character_varying": "ASDFGHJKL",
			"c_cidr": "192.168.100.128/25",
			"c_date": 18263,
			"c_daterange": "[2010-01-01,2010-01-04)",
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
			"c_inet_ipv4_no_subnet": "192.168.1.5",
			"c_inet_ipv4_subnet": "192.168.1.5/24",
			"c_inet_ipv6_no_subnet": "2001:4f8:3:ba:2e0:81ff:fe22:d1f1",
			"c_inet_ipv6_subnet": "2001:4f8:3:ba:2e0:81ff:fe22:d1f1/64",
			"c_int4range": "[10,20)",
			"c_int8range": "[1009900990099009000,9009900990099009000)",
			"c_int_array": [
				0,
				2,
				4,
				6
			],
			"c_integer": 12345,
			"c_interval": 7200000000,
			"c_json": "{\"foo\": \"bar\", \"baz\": 1234}",
			"c_jsonb": "{\"baz\": 4321, \"foo\": \"bar\"}",
			"c_macaddr": "12:34:56:78:90:ab",
			"c_macaddr8": "12:34:56:78:90:ab:cd:ef",
			"c_money": "T30t",
			"c_numeric": "AYHN",
			"c_numeric_nan": null,
			"c_numeric_variable": {
				"scale": 5,
				"value": "QX3UWQ=="
			},
			"c_numeric_variable_nan": null,
			"c_numrange": "[11.1,22.2)",
			"c_point": {
				"x": 12.34,
				"y": 56.78
			},
			"c_real": 45.678,
			"c_serial": 1000000123,
			"c_smallint": 32767,
			"c_text": "QWERTYUIOP",
			"c_time_with_timezone": "10:34:17.746572Z",
			"c_time_without_timezone": 45296000,
			"c_timestamp_with_timezone": "2001-02-16T13:38:40Z",
			"c_timestamp_without_timezone": 982355920000000,
			"c_tsrange": "[\"2010-01-01 14:30:00\",\"2010-01-01 15:30:00\")",
			"c_tstzrange": "[\"2001-02-16 08:38:40+00\",\"2001-03-20 08:38:40+00\")",
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

// testTypes checks that PostgreSQL data types are handled correctly.
func testTypes(db *sql.DB) error {
	tempTableName, dropTableFunc := utils.CreateTemporaryTable(db, testTypesCreateTableQuery)
	defer dropTableFunc()

	// Check reading an empty table
	_, err := readTable(db, tempTableName, 100)
	if err == nil {
		return fmt.Errorf("expected an error")
	} else if !errors.Is(err, rdbms.ErrNoPkValuesForEmptyTable) {
		return err
	}

	slog.Info("Inserting data...")
	if _, err := db.Exec(fmt.Sprintf(testTypesInsertQuery, tempTableName)); err != nil {
		return fmt.Errorf("unable to insert data: %w", err)
	}

	rows, err := readTable(db, tempTableName, 100)
	if err != nil {
		return err
	}

	if len(rows) != 1 {
		return fmt.Errorf("expected one row, got %d", len(rows))
	}
	row := rows[0]

	expectedPartitionKey := map[string]any{"pk": int64(1)}
	if !maps.Equal(row.PartitionKey(), expectedPartitionKey) {
		return fmt.Errorf("partition key %v does not match %v", row.PartitionKey(), expectedPartitionKey)
	}

	valueBytes, err := json.MarshalIndent(row.Event(), "", "\t")
	if err != nil {
		return fmt.Errorf("failed to marshal payload")
	}

	expectedPayload := fmt.Sprintf(expectedPayloadTemplate, utils.GetEvent(row).Payload.Source.TsMs, tempTableName)
	if utils.CheckDifference("payload", expectedPayload, string(valueBytes)) {
		return fmt.Errorf("payload does not match")
	}

	return nil
}

const testScanCreateTableQuery = `
CREATE TABLE %s (
	c_int_pk integer NOT NULL,
	c_boolean_pk boolean NOT NULL,
	c_text_pk text NOT NULL,
	c_text_value text,
	PRIMARY KEY (c_int_pk, c_boolean_pk, c_text_pk)
)
`

const testScanInsertQuery = `
INSERT INTO %s VALUES
(46, false, 'dj', 'row 0'),
(73, false, 'dr', 'row 1'),
(35, false, 'dr', 'row 2'),
(4, false, 'jn', 'row 3'),
(60, true, 'rj', 'row 4'),
(89, true, 'dn', 'row 5'),
(62, false, 'nn', 'row 6'),
(5, false, 'rn', 'row 7'),
(87, false, 'nr', 'row 8'),
(86, false, 'rn', 'row 9'),
(7, true, 'rr', 'row 10'),
(94, false, 'dn', 'row 11'),
(27, false, 'jr', 'row 12'),
(45, true, 'nr', 'row 13'),
(41, true, 'nr', 'row 14'),
(57, false, 'nj', 'row 15'),
(13, true, 'rd', 'row 16'),
(88, true, 'rj', 'row 17'),
(54, true, 'rd', 'row 18'),
(29, false, 'nr', 'row 19'),
(91, false, 'nj', 'row 20'),
(26, false, 'dr', 'row 21'),
(15, false, 'jr', 'row 22'),
(29, false, 'rj', 'row 23'),
(88, false, 'rr', 'row 24')
`

// testScan checks that we're fetching all the data from PostgreSQL.
func testScan(db *sql.DB) error {
	tempTableName, dropTableFunc := utils.CreateTemporaryTable(db, testScanCreateTableQuery)
	defer dropTableFunc()

	slog.Info("Inserting data...")
	if _, err := db.Exec(fmt.Sprintf(testScanInsertQuery, tempTableName)); err != nil {
		return fmt.Errorf("unable to insert data: %w", err)
	}

	expectedPartitionKeys := []map[string]any{
		{"c_int_pk": int64(4), "c_boolean_pk": false, "c_text_pk": "jn"},
		{"c_int_pk": int64(5), "c_boolean_pk": false, "c_text_pk": "rn"},
		{"c_int_pk": int64(7), "c_boolean_pk": true, "c_text_pk": "rr"},
		{"c_int_pk": int64(13), "c_boolean_pk": true, "c_text_pk": "rd"},
		{"c_int_pk": int64(15), "c_boolean_pk": false, "c_text_pk": "jr"},
		{"c_int_pk": int64(26), "c_boolean_pk": false, "c_text_pk": "dr"},
		{"c_int_pk": int64(27), "c_boolean_pk": false, "c_text_pk": "jr"},
		{"c_int_pk": int64(29), "c_boolean_pk": false, "c_text_pk": "nr"},
		{"c_int_pk": int64(29), "c_boolean_pk": false, "c_text_pk": "rj"},
		{"c_int_pk": int64(35), "c_boolean_pk": false, "c_text_pk": "dr"},
		{"c_int_pk": int64(41), "c_boolean_pk": true, "c_text_pk": "nr"},
		{"c_int_pk": int64(45), "c_boolean_pk": true, "c_text_pk": "nr"},
		{"c_int_pk": int64(46), "c_boolean_pk": false, "c_text_pk": "dj"},
		{"c_int_pk": int64(54), "c_boolean_pk": true, "c_text_pk": "rd"},
		{"c_int_pk": int64(57), "c_boolean_pk": false, "c_text_pk": "nj"},
		{"c_int_pk": int64(60), "c_boolean_pk": true, "c_text_pk": "rj"},
		{"c_int_pk": int64(62), "c_boolean_pk": false, "c_text_pk": "nn"},
		{"c_int_pk": int64(73), "c_boolean_pk": false, "c_text_pk": "dr"},
		{"c_int_pk": int64(86), "c_boolean_pk": false, "c_text_pk": "rn"},
		{"c_int_pk": int64(87), "c_boolean_pk": false, "c_text_pk": "nr"},
		{"c_int_pk": int64(88), "c_boolean_pk": false, "c_text_pk": "rr"},
		{"c_int_pk": int64(88), "c_boolean_pk": true, "c_text_pk": "rj"},
		{"c_int_pk": int64(89), "c_boolean_pk": true, "c_text_pk": "dn"},
		{"c_int_pk": int64(91), "c_boolean_pk": false, "c_text_pk": "nj"},
		{"c_int_pk": int64(94), "c_boolean_pk": false, "c_text_pk": "dn"},
	}
	expectedValues := []string{
		"row 3",
		"row 7",
		"row 10",
		"row 16",
		"row 22",
		"row 21",
		"row 12",
		"row 19",
		"row 23",
		"row 2",
		"row 14",
		"row 13",
		"row 0",
		"row 18",
		"row 15",
		"row 4",
		"row 6",
		"row 1",
		"row 9",
		"row 8",
		"row 24",
		"row 17",
		"row 5",
		"row 20",
		"row 11",
	}

	for _, batchSize := range []int{1, 2, 5, 6, 24, 25, 26} {
		slog.Info(fmt.Sprintf("Testing scan with batch size of %d...", batchSize))
		rows, err := readTable(db, tempTableName, batchSize)
		if err != nil {
			return err
		}
		if len(rows) != len(expectedPartitionKeys) {
			return fmt.Errorf("expected %d rows, got %d, batch size %d", len(expectedPartitionKeys), len(rows), batchSize)
		}
		for i, row := range rows {
			if !maps.Equal(row.PartitionKey(), expectedPartitionKeys[i]) {
				return fmt.Errorf("partition keys are different for row %d, batch size %d, %v != %v", i, batchSize, row.PartitionKey(), expectedPartitionKeys[i])
			}
			textValue := utils.GetEvent(row).Payload.After["c_text_value"]
			if textValue != expectedValues[i] {
				return fmt.Errorf("row values are different for row %d, batch size %d, %v != %v", i, batchSize, textValue, expectedValues[i])
			}
		}
	}

	return nil
}

const testPrimaryKeyTypesCreateTableQuery = `
CREATE TABLE %s (
	-- All the types from https://www.postgresql.org/docs/current/datatype.html#DATATYPE-TABLE
	c_bigint bigint,
	c_bigserial bigserial,
	c_bit bit,
	c_boolean boolean,
	c_bytea bytea,
	c_character character,
	c_character_varying character varying,
	c_cidr cidr,
	c_date date,
	c_double_precision double precision,
	c_inet inet,
	c_integer integer,
	c_interval interval,
	c_jsonb jsonb,
	c_macaddr macaddr,
	c_macaddr8 macaddr8,
	c_money money,
	c_numeric numeric(7, 2),
	c_real real,
	c_smallint smallserial,
	c_serial serial,
	c_text text,
	c_time_without_timezone time WITHOUT TIME ZONE,
	c_timestamp_without_timezone timestamp WITHOUT TIME ZONE,
	c_timestamp_with_timezone timestamp WITH TIME ZONE,
	c_uuid uuid,
	-- Range types from https://www.postgresql.org/docs/current/rangetypes.html
	c_int4range int4range,
	c_int8range int8range,
	c_numrange numrange,
	c_tsrange tsrange,
	c_tstzrange tstzrange,
	c_daterange daterange,
	PRIMARY KEY (
		c_bigint, c_bigserial, c_bit, c_boolean, c_bytea, c_character, c_character_varying, c_cidr, c_date, c_double_precision,
		c_inet, c_integer, c_interval, c_jsonb, c_macaddr, c_macaddr8, c_money, c_numeric, c_real, c_smallint, c_serial,
		c_text, c_time_without_timezone, c_timestamp_without_timezone, c_timestamp_with_timezone, c_uuid, c_int4range,
		c_int8range, c_numrange, c_tsrange, c_tstzrange, c_daterange
	)
)
`

const testPrimaryKeyTypesInsertQuery = `
INSERT INTO %s VALUES (
	-- c_bigint
		9009900990099009000,
	-- c_bigserial
		100000123100000123,
	-- c_bit
		B'1',
	-- c_boolean
		true,
	-- c_bytea
		'abc \153\154\155 \052\251\124'::bytea,
	-- c_character
		'X',
	-- c_character_varying
		'ASDFGHJKL',
	-- c_cidr
		'192.168.100.128/25',
	-- c_date
		'2020-01-02',
	-- c_double_precision
		123.456,
	-- c_inet
		'192.168.1.5',
	-- c_integer
		12345,
	-- c_interval
		'2 mon 3 day 4 hours',
	-- c_jsonb
		'{"foo": "bar", "baz": 4321}'::jsonb,
	-- c_macaddr
		'12:34:56:78:90:ab',
	-- c_macaddr8
		'12:34:56:78:90:ab:cd:ef',
	-- c_money
		'$52,093.89',
	-- c_numeric
		'987.654',
	-- c_real
		45.678,
	-- c_smallint
		32767,
	-- c_serial
		1000000123,
	-- c_text
		'QWERTYUIOP',
	-- c_time_without_timezone
		'20:38:21',
	-- c_timestamp_without_timezone
		'2001-02-16 20:38:40.123123',
	-- c_timestamp_with_timezone
		'2001-02-16 20:38:40.123123' AT TIME ZONE 'America/Denver',
	-- c_uuid
		'e7082e96-7190-4cc3-8ab4-bd27f1269f08',
	-- c_int4range
		int4range(10, 20),
	-- c_int8range
		int8range(1009900990099009000, 9009900990099009000),
	-- c_numrange
		numrange(11.1, 22.2),
	-- c_tsrange
		'[2010-01-01 14:30, 2010-01-01 15:30)',
	-- c_tstzrange
		tstzrange('2001-02-16 20:38:40+12', '2001-03-20 20:38:40+12'),
	-- c_daterange
		'[2010-01-01, 2010-01-03]'
)
`

// testPrimaryKeyTypes checks that we're able to handle primary keys with different types.
func testPrimaryKeyTypes(db *sql.DB) error {
	tempTableName, dropTableFunc := utils.CreateTemporaryTable(db, testPrimaryKeyTypesCreateTableQuery)
	defer dropTableFunc()

	slog.Info("Inserting data...")
	if _, err := db.Exec(fmt.Sprintf(testPrimaryKeyTypesInsertQuery, tempTableName)); err != nil {
		return fmt.Errorf("unable to insert data: %w", err)
	}

	rows, err := readTable(db, tempTableName, 100)
	if err != nil {
		return err
	}

	if len(rows) != 1 {
		return fmt.Errorf("expected one row, got %d", len(rows))
	}

	return nil
}
