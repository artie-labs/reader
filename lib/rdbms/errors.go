package rdbms

import (
	"errors"
)

var ErrPkValuesEmptyTable = errors.New("cannot get primary key values of empty table")
