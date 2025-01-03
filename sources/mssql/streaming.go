package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/writers"
)

type Streamer struct {
	cfg config.MSSQL
	db  *sql.DB
}

func (s *Streamer) Close() error {
	return s.db.Close()
}

func (s *Streamer) Run(ctx context.Context, writer writers.Writer) error {
	query := fmt.Sprintf("SELECT * FROM fn_dblog(NULL, NULL)")
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query transaction log: %w", err)
	}

	defer rows.Close()

	var count int
	for rows.Next() {
		// Process each row from the transaction log
		// This is a simplified example, you need to map the log columns to your data structure
		var logRecord map[string]interface{}
		if err = rows.Scan(&logRecord); err != nil {
			return err
			//return count, fmt.Errorf("failed to scan log record: %w", err)
		}

		count++
		fmt.Println("logRecord: ", logRecord, "count", count)
	}

	return nil
}
