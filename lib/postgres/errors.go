package postgres

func NoRowsError(err error) bool {
	if err != nil {
		return err.Error() == "sql: no rows in result set"
	}

	return false
}
