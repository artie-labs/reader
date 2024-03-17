package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"

	"github.com/lmittmann/tint"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/integration_tests/utils"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/lib/rdbms"
	"github.com/artie-labs/reader/sources/mysql/adapter"
)

func main() {
	os.Setenv("TZ", "UTC")
	slog.SetDefault(slog.New(tint.NewHandler(os.Stderr, &tint.Options{Level: slog.LevelInfo})))

	var mysqlHost string = os.Getenv("MYSQL_HOST")
	if mysqlHost == "" {
		mysqlHost = "127.0.0.1"
	}

	var mysqlCfg = config.MySQL{
		Host:     mysqlHost,
		Port:     3306,
		Username: "root",
		Password: "mysql",
		Database: "mysql",
	}

	db, err := sql.Open("mysql", mysqlCfg.ToDSN())
	if err != nil {
		logger.Fatal("Could not connect to MySQL", slog.Any("err", err))
	}

	// Modify sql_mode so that we can use '0000-00-00' dates
	_, err = db.Exec("SET SESSION sql_mode = ''")
	if err != nil {
		logger.Fatal("Unable to change sql_mode", slog.Any("err", err))
	}

	if err = testTypes(db); err != nil {
		logger.Fatal("Types test failed", slog.Any("err", err))
	}

	if err = testScan(db); err != nil {
		logger.Fatal("Scan test failed", slog.Any("err", err))
	}
}

func readTable(db *sql.DB, tableName string, batchSize int) ([]lib.RawMessage, error) {
	tableCfg := config.MySQLTable{
		Name:      tableName,
		BatchSize: uint(batchSize),
	}

	dbzAdapter, err := adapter.NewMySQLAdapter(db, tableCfg)
	if err != nil {
		return nil, err
	}

	return utils.ReadTable(db, dbzAdapter)
}

const testTypesCreateTableQuery = `
CREATE TABLE %s (
	pk INTEGER PRIMARY KEY NOT NULL,
	c_tinyint TINYINT,
	c_smallint SMALLINT,
	c_mediumint MEDIUMINT,
	c_int INT,
	c_bigint BIGINT,
	c_decimal DECIMAL(7, 5),
	c_numeric NUMERIC(5, 3),
	c_float FLOAT,
	c_double DOUBLE,
	c_bit BIT,
	c_boolean BOOLEAN,
	c_date DATE,
	c_date_0000_00_00 DATE,
	c_datetime DATETIME,
	c_timestamp TIMESTAMP,
	c_time TIME,
	c_year YEAR,
	c_char CHAR,
	c_varchar VARCHAR(100),
	c_binary BINARY(100),
	c_varbinary VARBINARY(100),
	c_blob BLOB,
	c_text TEXT,
	c_enum ENUM('x-small', 'small', 'medium', 'large', 'x-large'),
	c_set SET('one', 'two', 'three'),
	c_json JSON
)
`

const testTypesInsertQuery = `
INSERT INTO %s VALUES (
	-- pk
		1,
	-- c_tinyint
		1,
	-- c_smallint
		2,
	-- c_mediumint
		3,
	-- c_int
		4,
	-- c_bigint
		5,
	-- c_decimal
		'12.34',
	-- c_numeric
		'56.78',
	-- c_float
		90.123,
	-- c_double
		45.678,
	-- c_bit
		1,
	-- c_boolean
		false,
	-- c_date
		'2020-01-02',
	-- c_date_0000_00_00
		'0',
	-- c_datetime
		'2001-02-03 04:05:06',
	-- c_timestamp
		'2001-02-03 04:05:06',
	-- c_time
		'04:05:06',
	-- c_year
		'2001',
	-- c_char
		'X',
	-- c_varchar
		'GHJKL',
	-- c_binary
		'ASDF',
	-- c_varbinary
		'BNM',
	-- c_blob
		'QWER',
	-- c_text
		'ZXCV',
	-- c_enum
		'medium',
	-- c_set
		'one,two',
	-- c_json
		'{"key1": "value1", "key2": "value2"}'
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
						"type": "int16",
						"optional": false,
						"default": null,
						"field": "c_tinyint",
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
						"field": "c_mediumint",
						"name": "",
						"parameters": null
					},
					{
						"type": "int32",
						"optional": false,
						"default": null,
						"field": "c_int",
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
						"type": "bytes",
						"optional": false,
						"default": null,
						"field": "c_decimal",
						"name": "org.apache.kafka.connect.data.Decimal",
						"parameters": {
							"connect.decimal.precision": "7",
							"scale": "5"
						}
					},
					{
						"type": "",
						"optional": false,
						"default": null,
						"field": "c_numeric",
						"name": "org.apache.kafka.connect.data.Decimal",
						"parameters": {
							"connect.decimal.precision": "5",
							"scale": "3"
						}
					},
					{
						"type": "float",
						"optional": false,
						"default": null,
						"field": "c_float",
						"name": "",
						"parameters": null
					},
					{
						"type": "double",
						"optional": false,
						"default": null,
						"field": "c_double",
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
						"type": "int32",
						"optional": false,
						"default": null,
						"field": "c_date_0000_00_00",
						"name": "io.debezium.time.Date",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_datetime",
						"name": "io.debezium.time.Timestamp",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_timestamp",
						"name": "io.debezium.time.Timestamp",
						"parameters": null
					},
					{
						"type": "int64",
						"optional": false,
						"default": null,
						"field": "c_time",
						"name": "io.debezium.time.MicroTime",
						"parameters": null
					},
					{
						"type": "int32",
						"optional": false,
						"default": null,
						"field": "c_year",
						"name": "io.debezium.time.Year",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_char",
						"name": "",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_varchar",
						"name": "",
						"parameters": null
					},
					{
						"type": "bytes",
						"optional": false,
						"default": null,
						"field": "c_binary",
						"name": "",
						"parameters": null
					},
					{
						"type": "bytes",
						"optional": false,
						"default": null,
						"field": "c_varbinary",
						"name": "",
						"parameters": null
					},
					{
						"type": "bytes",
						"optional": false,
						"default": null,
						"field": "c_blob",
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
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_enum",
						"name": "io.debezium.data.Enum",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_set",
						"name": "io.debezium.data.EnumSet",
						"parameters": null
					},
					{
						"type": "string",
						"optional": false,
						"default": null,
						"field": "c_json",
						"name": "io.debezium.data.Json",
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
			"c_bigint": 5,
			"c_binary": "QVNERgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==",
			"c_bit": true,
			"c_blob": "UVdFUg==",
			"c_boolean": false,
			"c_char": "X",
			"c_date": 18263,
			"c_date_0000_00_00": null,
			"c_datetime": "2001-02-03T04:05:06Z",
			"c_decimal": "EtRQ",
			"c_double": 45.678,
			"c_enum": "medium",
			"c_float": 90.123,
			"c_int": 4,
			"c_json": "{\"key1\": \"value1\", \"key2\": \"value2\"}",
			"c_mediumint": 3,
			"c_numeric": "AN3M",
			"c_set": "one,two",
			"c_smallint": 2,
			"c_text": "ZXCV",
			"c_time": 14706000000,
			"c_timestamp": "2001-02-03T04:05:06Z",
			"c_tinyint": 1,
			"c_varbinary": "Qk5N",
			"c_varchar": "GHJKL",
			"c_year": 2001,
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

// testTypes checks that MySQL data types are handled correctly.
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
	if !maps.Equal(row.PartitionKey, expectedPartitionKey) {
		return fmt.Errorf("partition key %v does not match %v", row.PartitionKey, expectedPartitionKey)
	}

	valueBytes, err := json.MarshalIndent(row.GetPayload(), "", "\t")
	if err != nil {
		return fmt.Errorf("failed to marshal payload")
	}

	expectedPayload := fmt.Sprintf(expectedPayloadTemplate, utils.GetPayload(row).Payload.Source.TsMs, tempTableName)
	if utils.CheckDifference("payload", expectedPayload, string(valueBytes)) {
		return fmt.Errorf("payload does not match")
	}

	return nil
}

const testScanCreateTableQuery = `
CREATE TABLE %s (
	c_int_pk integer NOT NULL,
	c_boolean_pk boolean NOT NULL,
	c_text_pk VARCHAR(2) NOT NULL,
	c_text_value text,
	PRIMARY KEY(c_int_pk, c_boolean_pk, c_text_pk)
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

// testScan checks that we're fetching all the data from MySQL.
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
			if !maps.Equal(row.PartitionKey, expectedPartitionKeys[i]) {
				return fmt.Errorf("partition keys are different for row %d, batch size %d, %v != %v", i, batchSize, row.PartitionKey, expectedPartitionKeys[i])
			}
			textValue := utils.GetPayload(row).Payload.After["c_text_value"]
			if textValue != expectedValues[i] {
				return fmt.Errorf("row values are different for row %d, batch size %d, %v != %v", i, batchSize, textValue, expectedValues[i])
			}
		}
	}

	return nil
}
