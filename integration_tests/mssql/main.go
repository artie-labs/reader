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
	_ "github.com/microsoft/go-mssqldb"

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

	db, err := sql.Open("mssql", mssqlCfg.ToDSN())
	if err != nil {
		logger.Fatal("Could not connect to SQL Server", slog.Any("err", err))
	}

	if err = testTypes(db, mssqlCfg.Database); err != nil {
		logger.Fatal("Types test failed", slog.Any("err", err))
	}

	slog.Info("Test succeeded 😎")
}

func readTable(db *sql.DB, dbName, tableName string, batchSize int) ([]lib.RawMessage, error) {
	tableCfg := config.MSSQLTable{
		Schema:    "dbo",
		Name:      tableName,
		BatchSize: uint(batchSize),
	}

	dbzAdapter, err := adapter.NewMSSQLAdapter(db, dbName, tableCfg)
	if err != nil {
		return nil, err
	}

	return utils.ReadTable(dbzAdapter)
}

// TODO: Test every data type.
const testTypesCreateTableQuery = `
CREATE TABLE %s (
	pk INTEGER PRIMARY KEY NOT NULL,
	c_bit BIT,
	c_varbinary VARBINARY,
	c_smallint SMALLINT,
	c_int INT,
	c_bigint BIGINT
)
`

const testTypesInsertQuery = `
INSERT INTO %s VALUES (
	-- pk
	1,
	-- c_bit
	1,
	-- c_varbinary
	10101,
	-- c_smallint
	123,
	-- c_int
	1234,
	-- c_bigint
	1235
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
						"type": "boolean",
						"optional": false,
						"default": null,
						"field": "c_bit",
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
			"c_bit": true,
			"c_varbinary": "dQ==",
			"c_smallint": 123,
			"c_int": 1234,
			"c_bigint": 1235,
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
