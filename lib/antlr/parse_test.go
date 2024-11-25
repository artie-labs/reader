package antlr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDropTable(t *testing.T) {
	{
		// Single drop table
		events, err := Parse("DROP TABLE table_name;")
		assert.NoError(t, err)
		assert.Len(t, events, 1)

		dropTableEvent, isOk := events[0].(DropTableEvent)
		assert.True(t, isOk)

		assert.Len(t, dropTableEvent.GetTables(), 1)
		assert.Equal(t, "table_name", dropTableEvent.GetTables()[0])
	}
}
