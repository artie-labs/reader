package antlr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRenameTable(t *testing.T) {
	{
		// One table
		events, err := Parse(`RENAME TABLE table_b TO table_a;`)
		assert.NoError(t, err)
		assert.Len(t, events, 1)

		renameTableEvent, isOk := events[0].(RenameTableEvent)
		assert.True(t, isOk)

		assert.Equal(t, "table_b", renameTableEvent.GetTable())
		assert.Equal(t, "table_a", renameTableEvent.GetNewTableName())
	}
	{
		// Another one table variant
		events, err := Parse(`RENAME TABLE current_db.tbl_name TO other_db.tbl_name;`)
		assert.NoError(t, err)
		assert.Len(t, events, 1)

		renameTableEvent, isOk := events[0].(RenameTableEvent)
		assert.True(t, isOk)

		assert.Equal(t, "current_db.tbl_name", renameTableEvent.GetTable())
		assert.Equal(t, "other_db.tbl_name", renameTableEvent.GetNewTableName())
	}
	{
		// Multiple tables
		events, err := Parse(`RENAME TABLE old_table TO tmp_table, new_table TO old_table, tmp_table TO new_table;`)
		assert.NoError(t, err)
		assert.Len(t, events, 3)

		renameTableEvent1, isOk := events[0].(RenameTableEvent)
		assert.True(t, isOk)
		assert.Equal(t, "old_table", renameTableEvent1.GetTable())
		assert.Equal(t, "tmp_table", renameTableEvent1.GetNewTableName())

		renameTableEvent2, isOk := events[1].(RenameTableEvent)
		assert.True(t, isOk)
		assert.Equal(t, "new_table", renameTableEvent2.GetTable())
		assert.Equal(t, "old_table", renameTableEvent2.GetNewTableName())

		renameTableEvent3, isOk := events[2].(RenameTableEvent)
		assert.True(t, isOk)
		assert.Equal(t, "tmp_table", renameTableEvent3.GetTable())
		assert.Equal(t, "new_table", renameTableEvent3.GetNewTableName())
	}
}
