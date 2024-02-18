package postgres

import (
	"database/sql"
	"errors"
)

func NoRowsError(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
