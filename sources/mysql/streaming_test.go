package mysql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStreaming_ShouldProcessTable(t *testing.T) {
	s := Streaming{
		includedTablesMap: map[string]bool{
			"table1": true,
			"table2": true,
		},
	}

	assert.True(t, s.shouldProcessTable("table1"))
	assert.False(t, s.shouldProcessTable("table3"))
}
