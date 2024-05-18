package rdbms

func QueryPlaceholders(placeholder string, count int) []string {
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = placeholder
	}
	return placeholders
}
