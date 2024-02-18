package rdbms

import (
	"database/sql"
	"errors"
)

func IsNoRowsErr(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
