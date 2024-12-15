package antlr

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func assertOneElement[T any](t *testing.T, expected T, actual []T, msgAndArgs ...any) {
	assert.Len(t, actual, 1, msgAndArgs)
	assert.Equal(t, expected, actual[0], msgAndArgs)
}

func retrieveColumnsFromCreateTableEvent(t *testing.T, singleEvent []Event) []Column {
	assert.Len(t, singleEvent, 1)

	createTableEvent, isOk := singleEvent[0].(CreateTableEvent)
	assert.True(t, isOk)

	return createTableEvent.GetColumns()
}

func TestColumn_DefaultValue(t *testing.T) {
	// Different ways to set a default value
	{
		{
			// No default value
			events, err := Parse("CREATE TABLE table_name (id INT);")
			assert.NoError(t, err)

			cols := retrieveColumnsFromCreateTableEvent(t, events)
			assert.Len(t, cols, 1)
			assert.Equal(t, Column{Name: "id", DataType: "INT", PrimaryKey: false}, cols[0])
		}
		{
			// Float
			events, err := Parse("CREATE TABLE table_name (price DECIMAL(10, 2) DEFAULT 99.99);")
			assert.NoError(t, err)

			cols := retrieveColumnsFromCreateTableEvent(t, events)
			assert.Len(t, cols, 1)
			assert.Equal(t, Column{Name: "price", DataType: "DECIMAL(10,2)", DefaultValue: "99.99", PrimaryKey: false}, cols[0])
		}
		{
			// Integer
			events, err := Parse("CREATE TABLE table_name (id INT DEFAULT 0);")
			assert.NoError(t, err)

			cols := retrieveColumnsFromCreateTableEvent(t, events)
			assert.Len(t, cols, 1)
			assert.Equal(t, Column{Name: "id", DataType: "INT", DefaultValue: "0", PrimaryKey: false}, cols[0])
		}
		{
			// Boolean
			events, err := Parse("CREATE TABLE table_name (is_active BOOLEAN DEFAULT TRUE);")
			assert.NoError(t, err)

			cols := retrieveColumnsFromCreateTableEvent(t, events)
			assert.Len(t, cols, 1)
			assert.Equal(t, Column{Name: "is_active", DataType: "BOOLEAN", DefaultValue: "TRUE", PrimaryKey: false}, cols[0])
		}
		{
			// CURRENT_TIMESTAMP (ignored)
			events, err := Parse("CREATE TABLE table_name (created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);")
			assert.NoError(t, err)

			cols := retrieveColumnsFromCreateTableEvent(t, events)
			assert.Len(t, cols, 1)
			assert.Equal(t, Column{Name: "created_at", DataType: "TIMESTAMP", DefaultValue: "", PrimaryKey: false}, cols[0])
		}
		{
			// String
			events, err := Parse("CREATE TABLE table_name (name VARCHAR(50) DEFAULT 'default_name');")
			assert.NoError(t, err)

			cols := retrieveColumnsFromCreateTableEvent(t, events)
			assert.Len(t, cols, 1)
			assert.Equal(t, Column{Name: "name", DataType: "VARCHAR(50)", DefaultValue: "default_name", PrimaryKey: false}, cols[0])
		}
		{
			// Enum
			events, err := Parse("CREATE TABLE table_name (status ENUM('active', 'inactive', 'pending') DEFAULT 'active');")
			assert.NoError(t, err)

			cols := retrieveColumnsFromCreateTableEvent(t, events)
			assert.Len(t, cols, 1)
			assert.Equal(t, Column{Name: "status", DataType: "ENUM('active','inactive','pending')", DefaultValue: "active", PrimaryKey: false}, cols[0])
		}
		{
			// JSON
			events, err := Parse(`CREATE TABLE table_name (config JSON DEFAULT '{"key": "value"}');`)
			assert.NoError(t, err)

			cols := retrieveColumnsFromCreateTableEvent(t, events)
			assert.Len(t, cols, 1)
			assert.Equal(t, Column{Name: "config", DataType: "JSON", DefaultValue: `{"key": "value"}`, PrimaryKey: false}, cols[0])
		}
	}
}

// TestAlterTable - These queries are generated from: https://github.com/antlr/grammars-v4/blob/master/sql/mysql/Positive-Technologies/examples/ddl_alter.sql
func TestAlterTable(t *testing.T) {
	{
		// Irrelevant
		{
			// Truncating a table
			events, err := Parse("TRUNCATE TABLE foo;")
			assert.NoError(t, err)
			assert.Empty(t, events)
		}
		{
			// Dropping an index
			events, err := Parse("ALTER TABLE table_name DROP INDEX index_name;")
			assert.NoError(t, err)
			assert.Empty(t, events)
		}
		{
			// Adding an index
			events, err := Parse("ALTER TABLE table_name ADD INDEX index_name (col1, col2);")
			assert.NoError(t, err)
			assert.Empty(t, events)
		}
		{
			// Alter index
			events, err := Parse("alter table t3 alter index t3_i1 visible;")
			assert.NoError(t, err)
			assert.Empty(t, events)
		}
		{
			// Rename index
			events, err := Parse("alter table t3 rename index t3_i1 to t3_i2;")
			assert.NoError(t, err)
			assert.Empty(t, events)
		}
		{
			// Adding a constraint
			events, err := Parse("ALTER TABLE table_name ADD CONSTRAINT constraint_name UNIQUE (col1, col2);")
			assert.NoError(t, err)
			assert.Empty(t, events)
		}
		{
			// Dropping a constraint
			events, err := Parse("ALTER TABLE table_name DROP CONSTRAINT constraint_name;")
			assert.NoError(t, err)
			assert.Empty(t, events)
		}
		{
			// Dropping primary key (not supported for now)
			events, err := Parse("ALTER TABLE table_name DROP PRIMARY KEY;")
			assert.NoError(t, err)
			assert.Empty(t, events)
		}
		{
			// Renaming a table
			events, err := Parse("ALTER TABLE table_name RENAME TO new_table_name;")
			assert.NoError(t, err)
			assert.Empty(t, events)
		}
		{
			// Add partition
			events, err := Parse("alter table with_partition add partition (partition p201901 values less than (737425) engine = InnoDB);")
			assert.NoError(t, err)
			assert.Empty(t, events)
		}
		{
			// Recalculate stats
			events, err := Parse("alter table t1 stats_auto_recalc=default, stats_sample_pages=50;")
			assert.NoError(t, err)
			assert.Empty(t, events)
		}
		{
			// Dropping a foreign key
			events, err := Parse("alter table t1 drop foreign key fk1;")
			assert.NoError(t, err)
			assert.Empty(t, events)
		}
		{
			// Adding a foreign key
			events, err := Parse("alter table t1 add foreign key (c1) references t2 (c2);")
			assert.NoError(t, err)
			assert.Empty(t, events)
		}
		{
			// Adding a check
			events, err := Parse("alter table t1 add check (c1 > 0);")
			assert.NoError(t, err)
			assert.Empty(t, events)
		}
		{
			// Altering a check
			events, err := Parse("alter table t1 alter check c1 > 0;")
			assert.NoError(t, err)
			assert.Empty(t, events)
		}
	}
	{
		tableNames := []string{
			"table_name",             // Not escaping
			"`table_name`",           // Escaping table name
			"db_name.table_name",     // Not escaping
			"db_name.`table_name`",   // Escaping table name
			"`db_name`.table_name",   // Escaping db name
			"`db_name`.`table_name`", // Escaping both
		}

		// Fully qualified name
		for _, tblName := range tableNames {
			events, err := Parse(fmt.Sprintf("ALTER TABLE %s ADD COLUMN id INT;", tblName))
			assert.NoError(t, err, tblName)
			assert.Len(t, events, 1, tblName)

			addColEvent, isOk := events[0].(AddColumnsEvent)
			assert.True(t, isOk, tblName)
			assert.Equal(t, "table_name", addColEvent.GetTable(), tblName)
			assertOneElement(t, Column{Name: "id", DataType: "INT", PrimaryKey: false}, addColEvent.GetColumns(), tblName)
		}
	}
	{
		// Change the position of a column
		{
			// By modifying
			events, err := Parse("ALTER TABLE employees MODIFY COLUMN salary DECIMAL(10, 2) FIRST;")
			assert.NoError(t, err)
			assert.Len(t, events, 1)

			modifyColEvent, isOk := events[0].(ModifyColumnEvent)
			assert.True(t, isOk)
			assert.Equal(t, "employees", modifyColEvent.GetTable())
			assertOneElement(t, Column{Name: "salary", DataType: "DECIMAL(10,2)", PrimaryKey: false, Position: FirstPosition{}}, modifyColEvent.GetColumns())
		}
		{
			// By adding new columns
			events, err := Parse("alter table ship_class add column ship_spec varchar(150) first, add somecol int after start_build;")
			assert.NoError(t, err)
			assert.Len(t, events, 2)

			addColEvent1, isOk := events[0].(AddColumnsEvent)
			assert.True(t, isOk)
			assert.Equal(t, "ship_class", addColEvent1.GetTable())
			assertOneElement(t, Column{Name: "ship_spec", DataType: "varchar(150)", PrimaryKey: false, Position: FirstPosition{}}, addColEvent1.GetColumns())

			addColEvent2, isOk := events[1].(AddColumnsEvent)
			assert.True(t, isOk)
			assert.Equal(t, "ship_class", addColEvent2.GetTable())
			assertOneElement(t, Column{Name: "somecol", DataType: "int", PrimaryKey: false, Position: AfterPosition{column: "start_build"}}, addColEvent2.GetColumns())
		}
	}
	{
		// Adding columns
		{
			// Adding a column and index, but we only care about the column
			events, err := Parse("alter table t3 add column (c2 decimal(10, 2) comment 'comment`' null, c3 enum('abc', 'cba', 'aaa')), ADD COLUMN c4 varchar(255) first, add index t3_i1 using btree (c2) comment 'some index';")
			assert.NoError(t, err)
			assert.Len(t, events, 2)

			{
				// First event
				addColEvent, isOk := events[0].(AddColumnsEvent)
				assert.True(t, isOk)
				assert.Equal(t, "t3", addColEvent.GetTable())
				assert.Len(t, addColEvent.GetColumns(), 2)

				assert.Equal(t, Column{Name: "c2", DataType: "decimal(10,2)", PrimaryKey: false}, addColEvent.GetColumns()[0])
				assert.Equal(t, Column{Name: "c3", DataType: "enum('abc','cba','aaa')", PrimaryKey: false}, addColEvent.GetColumns()[1])
			}
			{
				// Second event
				addColEvent, isOk := events[1].(AddColumnsEvent)
				assert.True(t, isOk)
				assert.Equal(t, "t3", addColEvent.GetTable())
				assert.Len(t, addColEvent.GetColumns(), 1)

				assert.Equal(t, Column{Name: "c4", DataType: "varchar(255)", PrimaryKey: false, Position: FirstPosition{}}, addColEvent.GetColumns()[0])
			}
		}
		{
			// Adding a column without specifying "column"
			events, err := Parse("ALTER TABLE `order` ADD cancelled TINYINT(1) DEFAULT 0 NOT NULL, ADD delivered TINYINT(1) DEFAULT 0 NOT NULL, ADD returning TINYINT(1) DEFAULT 0 NOT NULL;")
			assert.NoError(t, err)
			assert.Len(t, events, 3)

			addColEvent1, isOk := events[0].(AddColumnsEvent)
			assert.True(t, isOk)
			assert.Equal(t, "order", addColEvent1.GetTable())
			assertOneElement(t, Column{Name: "cancelled", DataType: "TINYINT(1)", DefaultValue: "0", PrimaryKey: false}, addColEvent1.GetColumns())

			addColEvent2, isOk := events[1].(AddColumnsEvent)
			assert.True(t, isOk)
			assert.Equal(t, "order", addColEvent2.GetTable())
			assertOneElement(t, Column{Name: "delivered", DataType: "TINYINT(1)", DefaultValue: "0", PrimaryKey: false}, addColEvent2.GetColumns())

			addColEvent3, isOk := events[2].(AddColumnsEvent)
			assert.True(t, isOk)
			assert.Equal(t, "order", addColEvent3.GetTable())
			assertOneElement(t, Column{Name: "returning", DataType: "TINYINT(1)", DefaultValue: "0", PrimaryKey: false}, addColEvent3.GetColumns())
		}
		{
			// Adding column + including a comment
			events, err := Parse("alter table default.task add column xxxx varchar(200) comment 'cdc test';")
			assert.NoError(t, err)
			assert.Len(t, events, 1)

			addColEvent, isOk := events[0].(AddColumnsEvent)
			assert.True(t, isOk)
			assert.Equal(t, "task", addColEvent.GetTable())
			assertOneElement(t, Column{Name: "xxxx", DataType: "varchar(200)", PrimaryKey: false}, addColEvent.GetColumns())
		}
		{
			// Adding a column, and making that column a primary key
			events, err := Parse("alter table goods add column `id` int(10) unsigned primary KEY AUTO_INCREMENT;")
			assert.NoError(t, err)
			assert.Len(t, events, 1)

			addColEvent, isOk := events[0].(AddColumnsEvent)
			assert.True(t, isOk)
			assert.Equal(t, "goods", addColEvent.GetTable())
			assertOneElement(t, Column{Name: "id", DataType: "int(10) unsigned", PrimaryKey: true}, addColEvent.GetColumns())
		}
	}
	{
		// Renaming a column
		{
			// No escape
			events, err := Parse("alter table t5 rename column old to new;")
			assert.NoError(t, err)
			assert.Len(t, events, 1)

			renameColEvent, isOk := events[0].(RenameColumnEvent)
			assert.True(t, isOk)

			assert.Equal(t, "t5", renameColEvent.GetTable())
			assertOneElement(t, Column{Name: "new", PreviousName: "old", DataType: "", PrimaryKey: false}, renameColEvent.GetColumns())
		}
		{
			// Escape
			events, err := Parse("alter table t5 rename column `old` to `new`;")
			assert.NoError(t, err)
			assert.Len(t, events, 1)

			renameColEvent, isOk := events[0].(RenameColumnEvent)
			assert.True(t, isOk)

			assert.Equal(t, "t5", renameColEvent.GetTable())
			assertOneElement(t, Column{Name: "new", PreviousName: "old", DataType: "", PrimaryKey: false}, renameColEvent.GetColumns())
		}
	}
	{
		// Adding two columns
		events, err := Parse("ALTER TABLE table_name ADD COLUMN id INT, ADD COLUMN name VARCHAR(255);")
		assert.NoError(t, err)
		assert.Len(t, events, 2)

		addColEvent1, isOk := events[0].(AddColumnsEvent)
		assert.True(t, isOk)
		assert.Equal(t, "table_name", addColEvent1.GetTable())
		assertOneElement(t, Column{Name: "id", DataType: "INT", PrimaryKey: false}, addColEvent1.GetColumns())

		addColEvent2, isOk := events[1].(AddColumnsEvent)
		assert.True(t, isOk)
		assert.Equal(t, "table_name", addColEvent2.GetTable())
		assertOneElement(t, Column{Name: "name", DataType: "VARCHAR(255)", PrimaryKey: false}, addColEvent2.GetColumns())
	}
	{
		// Adding 2 columns (one escaped), and dropping 2 columns (one escaped)
		events, err := Parse("ALTER TABLE `table_name` ADD COLUMN `id` INT, ADD COLUMN name VARCHAR(255), DROP COLUMN `col1`, DROP COLUMN col2;")
		assert.NoError(t, err)
		assert.Len(t, events, 4)

		addColEvent1, isOk := events[0].(AddColumnsEvent)
		assert.True(t, isOk)
		assert.Equal(t, "table_name", addColEvent1.GetTable())
		assertOneElement(t, Column{Name: "id", DataType: "INT", PrimaryKey: false}, addColEvent1.GetColumns())

		addColEvent2, isOk := events[1].(AddColumnsEvent)
		assert.True(t, isOk)
		assert.Equal(t, "table_name", addColEvent2.GetTable())
		assertOneElement(t, Column{Name: "name", DataType: "VARCHAR(255)", PrimaryKey: false}, addColEvent2.GetColumns())

		dropColEvent1, isOk := events[2].(DropColumnsEvent)
		assert.True(t, isOk)
		assert.Equal(t, "table_name", dropColEvent1.GetTable())
		assertOneElement(t, Column{Name: "col1", DataType: "", PrimaryKey: false}, dropColEvent1.GetColumns())

		dropColEvent2, isOk := events[3].(DropColumnsEvent)
		assert.True(t, isOk)
		assert.Equal(t, "table_name", dropColEvent2.GetTable())
		assertOneElement(t, Column{Name: "col2", DataType: "", PrimaryKey: false}, dropColEvent2.GetColumns())
	}
	{
		// Changing a column data type
		events, err := Parse("ALTER TABLE table_name MODIFY COLUMN id VARCHAR(255);")
		assert.NoError(t, err)
		assert.Len(t, events, 1)

		modifyColEvent, isOk := events[0].(ModifyColumnEvent)
		assert.True(t, isOk)
		assert.Equal(t, "table_name", modifyColEvent.GetTable())
		assertOneElement(t, Column{Name: "id", DataType: "VARCHAR(255)", PrimaryKey: false}, modifyColEvent.GetColumns())
	}
	{
		// Drop, add and modify column
		events, err := Parse("ALTER TABLE table_name DROP COLUMN id, ADD COLUMN name VARCHAR(255), MODIFY COLUMN col1 INT;")
		assert.NoError(t, err)
		assert.Len(t, events, 3)

		dropColEvent, isOk := events[0].(DropColumnsEvent)
		assert.True(t, isOk)
		assert.Equal(t, "table_name", dropColEvent.GetTable())
		assertOneElement(t, Column{Name: "id", DataType: "", PrimaryKey: false}, dropColEvent.GetColumns())

		addColEvent, isOk := events[1].(AddColumnsEvent)
		assert.True(t, isOk)
		assert.Equal(t, "table_name", addColEvent.GetTable())
		assertOneElement(t, Column{Name: "name", DataType: "VARCHAR(255)", PrimaryKey: false}, addColEvent.GetColumns())

		modifyColEvent, isOk := events[2].(ModifyColumnEvent)
		assert.True(t, isOk)
		assert.Equal(t, "table_name", modifyColEvent.GetTable())
		assertOneElement(t, Column{Name: "col1", DataType: "INT", PrimaryKey: false}, modifyColEvent.GetColumns())
	}
	{
		// Primary keys
		{
			// Add a primary key with a name
			queries := []string{"alter table table1 add primary key `table_pk` (`id`);", "alter table table1 add primary key `table_pk` (id);"}
			for _, query := range queries {
				events, err := Parse(query)
				assert.NoError(t, err)
				assert.Len(t, events, 1)

				addPrimaryKeyEvent, isOk := events[0].(AddPrimaryKeyEvent)
				assert.True(t, isOk)
				assert.Equal(t, "table1", addPrimaryKeyEvent.GetTable())
				assertOneElement(t, Column{Name: "id", DataType: "", PrimaryKey: true}, addPrimaryKeyEvent.GetColumns())
			}
		}
		{
			// Make an existing column a primary key
			events, err := Parse("ALTER TABLE table_name ADD PRIMARY KEY (id);")
			assert.NoError(t, err)
			assert.Len(t, events, 1)

			addPrimaryKeyEvent, isOk := events[0].(AddPrimaryKeyEvent)
			assert.True(t, isOk)

			assert.Equal(t, "table_name", addPrimaryKeyEvent.GetTable())
			assertOneElement(t, Column{Name: "id", DataType: "", PrimaryKey: true}, addPrimaryKeyEvent.GetColumns())
		}
		{
			// Make two existing columns a primary key (one escaped, one not)
			events, err := Parse("ALTER TABLE table_name ADD PRIMARY KEY (`id`, name);")
			assert.NoError(t, err)
			assert.Len(t, events, 1)

			addPrimaryKeyEvent, isOk := events[0].(AddPrimaryKeyEvent)
			assert.True(t, isOk)

			assert.Equal(t, "table_name", addPrimaryKeyEvent.GetTable())
			assert.Len(t, addPrimaryKeyEvent.GetColumns(), 2)
			assert.Equal(t, Column{Name: "id", DataType: "", PrimaryKey: true}, addPrimaryKeyEvent.GetColumns()[0])
			assert.Equal(t, Column{Name: "name", DataType: "", PrimaryKey: true}, addPrimaryKeyEvent.GetColumns()[1])
		}
	}
}
