package streaming

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func initializeAdapter(t *testing.T) SchemaAdapter {
	adapter := SchemaAdapter{adapters: map[string]TableAdapter{}}
	// Create a table first
	assert.NoError(t, adapter.ApplyDDL("CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255));", 1337))

	// Check the table exists
	assert.Len(t, adapter.adapters, 1)
	assert.Len(t, adapter.adapters["test_table"].columns, 3)
	assert.Equal(t, Column{Name: "id", DataType: "INT", PrimaryKey: true}, adapter.adapters["test_table"].columns[0])
	assert.Equal(t, Column{Name: "name", DataType: "VARCHAR(255)"}, adapter.adapters["test_table"].columns[1])
	assert.Equal(t, Column{Name: "email", DataType: "VARCHAR(255)"}, adapter.adapters["test_table"].columns[2])
	assert.Equal(t, int64(1337), adapter.adapters["test_table"].unixTs)
	return adapter
}

func TestSchemaAdapter_ApplyDDL(t *testing.T) {
	{
		// Column rename
		adapter := initializeAdapter(t)
		{
			// Table does not exist
			err := adapter.ApplyDDL("ALTER TABLE non_existing_table RENAME COLUMN id TO new_id;", 0)
			assert.ErrorContains(t, err, `table not found: "non_existing_table"`)
		}
		{
			// Column does not exist
			err := adapter.ApplyDDL("ALTER TABLE test_table RENAME COLUMN non_existing_column TO new_id;", 0)
			assert.ErrorContains(t, err, `column not found: "non_existing_column"`)
		}
		{
			// Valid column rename
			assert.NoError(t, adapter.ApplyDDL("ALTER TABLE test_table RENAME COLUMN id TO new_id;", 123))
			assert.Len(t, adapter.adapters["test_table"].columns, 3)
			assert.Equal(t, Column{Name: "new_id", DataType: "INT", PrimaryKey: true}, adapter.adapters["test_table"].columns[0])
			assert.Equal(t, int64(123), adapter.adapters["test_table"].unixTs)
		}
	}
	{
		// Adding a primary key
		adapter := initializeAdapter(t)
		{
			// Table does not exist
			err := adapter.ApplyDDL("ALTER TABLE non_existing_table ADD PRIMARY KEY (id);", 0)
			assert.ErrorContains(t, err, `table not found: "non_existing_table"`)
		}
		{
			// Column does not exist
			err := adapter.ApplyDDL("ALTER TABLE test_table ADD PRIMARY KEY (non_existing_column);", 0)
			assert.ErrorContains(t, err, `column not found: "non_existing_column"`)
		}
		{
			// Valid primary key addition
			assert.NoError(t, adapter.ApplyDDL("ALTER TABLE test_table ADD PRIMARY KEY (name);", 9))
			assert.Len(t, adapter.adapters["test_table"].columns, 3)
			assert.Equal(t, Column{Name: "id", DataType: "INT", PrimaryKey: true}, adapter.adapters["test_table"].columns[0])
			assert.Equal(t, Column{Name: "name", DataType: "VARCHAR(255)", PrimaryKey: true}, adapter.adapters["test_table"].columns[1])
			assert.Equal(t, Column{Name: "email", DataType: "VARCHAR(255)"}, adapter.adapters["test_table"].columns[2])
			assert.Equal(t, int64(9), adapter.adapters["test_table"].unixTs)
		}
	}
	{
		// Modifying columns
		adapter := initializeAdapter(t)
		{
			// Table does not exist
			err := adapter.ApplyDDL("ALTER TABLE non_existing_table MODIFY COLUMN id VARCHAR(255);", 0)
			assert.ErrorContains(t, err, `table not found: "non_existing_table"`)
		}
		{
			// Column does not exist
			err := adapter.ApplyDDL("ALTER TABLE test_table MODIFY COLUMN non_existing_column VARCHAR(255);", 0)
			assert.ErrorContains(t, err, `column not found: "non_existing_column"`)
		}
		{
			// Applying one column type change
			assert.NoError(t, adapter.ApplyDDL("ALTER TABLE test_table MODIFY COLUMN id VARCHAR(255);", 999))
			assert.Len(t, adapter.adapters["test_table"].columns, 3)
			assert.Equal(t, Column{Name: "id", DataType: "VARCHAR(255)", PrimaryKey: true}, adapter.adapters["test_table"].columns[0])
			assert.Equal(t, int64(999), adapter.adapters["test_table"].unixTs)
		}
		{
			// Applying multiple column type changes
			assert.NoError(t, adapter.ApplyDDL("ALTER TABLE test_table MODIFY COLUMN id VARCHAR(255), MODIFY COLUMN name INT;", 1234))
			assert.Len(t, adapter.adapters["test_table"].columns, 3)
			assert.Equal(t, Column{Name: "id", DataType: "VARCHAR(255)", PrimaryKey: true}, adapter.adapters["test_table"].columns[0])
			assert.Equal(t, Column{Name: "name", DataType: "INT"}, adapter.adapters["test_table"].columns[1])
			assert.Equal(t, int64(1234), adapter.adapters["test_table"].unixTs)
		}
	}
	{
		// Dropping columns
		adapter := initializeAdapter(t)
		{
			// Table does not exist
			err := adapter.ApplyDDL("ALTER TABLE non_existing_table DROP COLUMN id;", 0)
			assert.ErrorContains(t, err, `table not found: "non_existing_table"`)
		}
		{
			// Column does not exist
			err := adapter.ApplyDDL("ALTER TABLE test_table DROP COLUMN non_existing_column;", 0)
			assert.ErrorContains(t, err, `column not found: "non_existing_column"`)
		}
		{
			// Dropping one column
			assert.NoError(t, adapter.ApplyDDL("ALTER TABLE test_table DROP COLUMN name;", 12))
			assert.Len(t, adapter.adapters["test_table"].columns, 2)
			assert.Equal(t, Column{Name: "id", DataType: "INT", PrimaryKey: true}, adapter.adapters["test_table"].columns[0])
			assert.Equal(t, Column{Name: "email", DataType: "VARCHAR(255)"}, adapter.adapters["test_table"].columns[1])
			assert.Equal(t, int64(12), adapter.adapters["test_table"].unixTs)
		}
		{
			// Dropping multiple columns
			assert.NoError(t, adapter.ApplyDDL("ALTER TABLE test_table DROP COLUMN id, DROP COLUMN email;", 1234))
			assert.Empty(t, adapter.adapters["test_table"].columns)
			assert.Equal(t, int64(1234), adapter.adapters["test_table"].unixTs)
		}
	}
	{
		// Adding columns
		adapter := initializeAdapter(t)
		{
			// Table does not exist
			err := adapter.ApplyDDL("ALTER TABLE non_existing_table ADD COLUMN id INT;", 0)
			assert.ErrorContains(t, err, `table not found: "non_existing_table"`)
		}
		{
			// Column already exists
			err := adapter.ApplyDDL("ALTER TABLE test_table ADD COLUMN id INT;", 0)
			assert.ErrorContains(t, err, `column already exists: "id"`)
		}
		{
			// Add one column
			assert.NoError(t, adapter.ApplyDDL("ALTER TABLE test_table ADD COLUMN new_column INT;", 9))
			assert.Len(t, adapter.adapters["test_table"].columns, 4)
			assert.Equal(t, Column{Name: "new_column", DataType: "INT"}, adapter.adapters["test_table"].columns[3])
			assert.Equal(t, int64(9), adapter.adapters["test_table"].unixTs)
		}
		{
			// Adding two columns
			assert.NoError(t, adapter.ApplyDDL("ALTER TABLE test_table ADD COLUMN new_column2 INT, ADD COLUMN new_column3 VARCHAR(255);", 99))
			assert.Len(t, adapter.adapters["test_table"].columns, 6)
			assert.Equal(t, Column{Name: "new_column2", DataType: "INT"}, adapter.adapters["test_table"].columns[4])
			assert.Equal(t, Column{Name: "new_column3", DataType: "VARCHAR(255)"}, adapter.adapters["test_table"].columns[5])
			assert.Equal(t, int64(99), adapter.adapters["test_table"].unixTs)
		}
	}
}
