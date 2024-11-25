package antlr

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDropTable(t *testing.T) {
	{
		// Single drop table
		for _, tblName := range []string{"table_name", "`table_name`"} {
			events, err := Parse(fmt.Sprintf("DROP TABLE %s;", tblName))
			assert.NoError(t, err)
			assert.Len(t, events, 1)

			dropTableEvent, isOk := events[0].(DropTableEvent)
			assert.True(t, isOk)

			assert.Len(t, dropTableEvent.GetTables(MySQLUnescapeFunction), 1)
			assert.Equal(t, "table_name", dropTableEvent.GetTables(MySQLUnescapeFunction)[0])
		}
	}
	{
		// Multiple drop table
		events, err := Parse("DROP TABLE table_name1, table_name2;")
		assert.NoError(t, err)
		assert.Len(t, events, 1)

		dropTableEvent, isOk := events[0].(DropTableEvent)
		assert.True(t, isOk)

		assert.Len(t, dropTableEvent.GetTables(MySQLUnescapeFunction), 2)
		assert.Equal(t, []string{"table_name1", "table_name2"}, dropTableEvent.GetTables(MySQLUnescapeFunction))
	}
}
