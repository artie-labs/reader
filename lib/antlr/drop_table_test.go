package antlr

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDropTable(t *testing.T) {
	{
		// Drop table specify schema
		events, err := Parse("DROP TABLE db_name.table_name;")
		assert.NoError(t, err)
		assert.Len(t, events, 1)

		dropTableEvent, isOk := events[0].(DropTableEvent)
		assert.True(t, isOk)
		assert.Equal(t, "table_name", dropTableEvent.GetTable())
	}
	{
		// Single drop table
		for _, tblName := range []string{"table_name", "`table_name`", "db_name.table_name", "`db_name`.`table_name`"} {
			events, err := Parse(fmt.Sprintf("DROP TABLE %s;", tblName))
			assert.NoError(t, err)
			assert.Len(t, events, 1)

			dropTableEvent, isOk := events[0].(DropTableEvent)
			assert.True(t, isOk)
			assert.Equal(t, "table_name", dropTableEvent.GetTable())
		}
	}
	{
		// Drop table if exists
		for _, tblName := range []string{"table_name", "`table_name`"} {
			events, err := Parse(fmt.Sprintf("DROP TABLE IF EXISTS %s;", tblName))
			assert.NoError(t, err)
			assert.Len(t, events, 1)

			dropTableEvent, isOk := events[0].(DropTableEvent)
			assert.True(t, isOk)
			assert.Equal(t, "table_name", dropTableEvent.GetTable())
		}
	}
	{
		// Multiple drop table
		events, err := Parse("DROP TABLE table_name1, table_name2, `table_name3`;")
		assert.NoError(t, err)
		assert.Len(t, events, 3)

		for index, tblName := range []string{"table_name1", "table_name2", "table_name3"} {
			dropTableEvent, isOk := events[index].(DropTableEvent)
			assert.True(t, isOk)
			assert.Equal(t, tblName, dropTableEvent.GetTable())
		}
	}
}
