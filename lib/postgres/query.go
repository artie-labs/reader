package postgres

import (
	"fmt"
)

func QueryPlaceholders(offset, count int) []string {
	result := make([]string, count)
	for i := range count {
		result[i] = fmt.Sprintf("$%d", 1+offset+i)
	}
	return result
}
