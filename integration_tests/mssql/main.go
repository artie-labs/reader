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

	"github.com/lmittmann/tint"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/integration_tests/utils"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/lib/rdbms"
	"github.com/artie-labs/reader/sources/mssql/adapter"
)

func main() {
	if err := os.Setenv("TZ", "UTC"); err != nil {
		logger.Fatal("Unable to set TZ env var: %w", err)
	}
	slog.SetDefault(slog.New(tint.NewHandler(os.Stderr, &tint.Options{})))
	mssqlCfg := config.MSSQL{
		Host:     cmp.Or(os.Getenv("MSSQL_HOST"), "127.0.0.1"),
		Port:     1433,
		Username: "sa",
		Password: "yourStrong!Password",
		Database: "master",
	}

	db, err := sql.Open("sqlserver", mssqlCfg.ToDSN())
	if err != nil {
		logger.Fatal("Could not connect to SQL Server", slog.Any("err", err))
	}

	if err = testTypes(db, mssqlCfg.Database); err != nil {
		logger.Fatal("Types test failed", slog.Any("err", err))
	}

	if err = testScan(db, mssqlCfg.Database); err != nil {
		logger.Fatal("Scan test failed", slog.Any("err", err))
	}

	slog.Info("Test succeeded 😎")
}

func readTable(db *sql.DB, dbName, tableName string, batchSize int) ([]lib.RawMessage, error) {
	tableCfg := config.MSSQLTable{
		Name:      tableName,
		BatchSize: uint(batchSize),
	}

	dbzAdapter, err := adapter.NewMSSQLAdapter(db, dbName, tableCfg)
	if err != nil {
		return nil, err
	}

	return utils.ReadTable(dbzAdapter)
}

const testTypesCreateTableQuery = `
CREATE TABLE %s (
	pk INT PRIMARY KEY NOT NULL,
	c_int INT,
	c_varchar VARCHAR(100),
	c_date DATE
)
`

const testTypesInsertQuery = `
INSERT INTO %s VALUES (
	1,
	123,
	'Test',
	'2020-01-01'
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
						"type": "int32",
						"optional": false,
						"default": null,
						"field": "c_int",
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
						"type": "int32",
						"optional": false,
						"default": null,
						"field": "c_date",
						"name": "io.debezium.time.Date",
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
			"pk": 1,
			"c_int": 123,
			"c_varchar": "Test",
			"c_date": 18262
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

func testTypes(db *sql.DB, dbName string) error {
	tempTableName, dropTableFunc := utils.CreateTemporaryTable(db, testTypesCreateTableQuery)
	defer dropTableFunc()

	// Check reading an empty table
	_, err := readTable(db, dbName, tempTableName, 100)
	if err == nil {
		return fmt.Errorf("expected an error")
	} else if !errors.Is(err, rdbms.ErrNoPkValuesForEmptyTable) {
		return err
	}

	slog.Info("Inserting data...")
	if _, err := db.Exec(fmt.Sprintf(testTypesInsertQuery, tempTableName)); err != nil {
		return fmt.Errorf("unable to insert data: %w", err)
	}

	rows, err := readTable(db, dbName, tempTableName, 100)
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
	c_int_pk INT NOT NULL,
	c_boolean_pk BIT NOT NULL,
	c_text_pk VARCHAR(2) NOT NULL,
	c_text_value TEXT,
	PRIMARY KEY(c_int_pk, c_boolean_pk, c_text_pk)
)
`

const testScanInsertQuery = `
INSERT INTO %s VALUES
(1, 0, 'A', 'row 1'),
(2, 1, 'B', 'row 2')
`

func testScan(db *sql.DB, dbName string) error {
	tempTableName, dropTableFunc := utils.CreateTemporaryTable(db, testScanCreateTableQuery)
	defer dropTableFunc()

	slog.Info("Inserting data...")
	if _, err := db.Exec(fmt.Sprintf(testScanInsertQuery, tempTableName)); err != nil {
		return fmt.Errorf("unable to insert data: %w", err)
	}

	expectedPartitionKeys := []map[string]any{
		{"c_int_pk": int64(1), "c_boolean_pk": int64(0), "c_text_pk": "A"},
		{"c_int_pk": int64(2), "c_boolean_pk": int64(1), "c_text_pk": "B"},
	}
	expectedValues := []string{
		"row 1",
		"row 2",
	}

	for _, batchSize := range []int{1, 2} {
		slog.Info(fmt.Sprintf("Testing scan with batch size of %d...", batchSize))
		rows, err := readTable(db, dbName, tempTableName, batchSize)
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
