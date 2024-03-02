package rdbms

import (
	"errors"
)

var ErrNoPkValuesForEmptyTable = errors.New("cannot get primary key values of empty table")
