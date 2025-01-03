package mssql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/writers"
	sql2 "github.com/artie-labs/transfer/lib/sql"
	"log/slog"
	"slices"
)

type Streamer struct {
	cfg config.MSSQL
	db  *sql.DB
}

var validOp = []string{
	"LOP_INSERT_ROWS",
	"LOP_MODIFY_ROW",
	"LOP_DELETE_ROWS",
}

func shouldProcessRow(row map[string]interface{}) bool {
	val, isOk := row["Operation"]
	if !isOk {
		slog.Warn("Skipping, operation not found in row")
		return false
	}

	operation, isOk := val.(string)
	if !isOk {
		slog.Warn("Skipping, operation is not a string", slog.String("type", fmt.Sprintf("%T", val)))
		return false
	}

	if slices.Contains(validOp, operation) {
		return true
	}

	slog.Warn("Skipping, invalid operation", slog.String("operation", operation))
	return false
}

func (s *Streamer) Close() error {
	return s.db.Close()
}

func (s *Streamer) Run(ctx context.Context, writer writers.Writer) error {
	query := fmt.Sprintf("SELECT * FROM fn_dblog(NULL, NULL)")
	sqlRows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query transaction log: %w", err)
	}

	rows, err := sql2.RowsToObjects(sqlRows)
	if err != nil {
		return fmt.Errorf("failed to convert rows to objects: %w", err)
	}

	for _, row := range rows {
		if !shouldProcessRow(row) {
			continue
		}

		fmt.Println("Row Details:")
		for key, value := range row {
			fmt.Println("Key", fmt.Sprintf("Type: %T", key))
			if value == nil {
				fmt.Printf("  %s: <nil>\n", key)
			} else {
				switch v := value.(type) {
				case []byte:
					// Convert binary data to a readable string (hex or UTF-8)
					fmt.Printf("  %s: %s\n", key, hex.EncodeToString(v))
				default:
					// Print other types directly
					fmt.Printf("  %s: %v\n", key, value)
				}
			}
		}
		fmt.Println()
	}
	return nil
}
