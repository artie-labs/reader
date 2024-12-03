package streaming

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func initializeAdapter(t *testing.T) SchemaAdapter {
	adapter := SchemaAdapter{adapters: map[string]TableAdapter{}}
	// Create a table first
	assert.NoError(t, adapter.ApplyDDL("CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255));"))

	// Check the table exists
	assert.Equal(t, 1, len(adapter.adapters))
	assert.Equal(t, 3, len(adapter.adapters["test_table"].columns))
	assert.Equal(t, Column{Name: "id", DataType: "INT", PrimaryKey: true}, adapter.adapters["test_table"].columns[0])
	assert.Equal(t, Column{Name: "name", DataType: "VARCHAR(255)"}, adapter.adapters["test_table"].columns[1])
	assert.Equal(t, Column{Name: "email", DataType: "VARCHAR(255)"}, adapter.adapters["test_table"].columns[2])
	return adapter
}

func TestSchemaAdapter_ApplyDDL(t *testing.T) {
	{
		// Column rename
		adapter := initializeAdapter(t)
		{
			// Table does not exist
			err := adapter.ApplyDDL("ALTER TABLE non_existing_table RENAME COLUMN id TO new_id;")
			assert.ErrorContains(t, err, `table not found: "non_existing_table"`)
		}
		{
			// Column does not exist
			err := adapter.ApplyDDL("ALTER TABLE test_table RENAME COLUMN non_existing_column TO new_id;")
			assert.ErrorContains(t, err, `column not found: "non_existing_column"`)
		}
		{
			// Valid column rename
			assert.NoError(t, adapter.ApplyDDL("ALTER TABLE test_table RENAME COLUMN id TO new_id;"))
			assert.Equal(t, 3, len(adapter.adapters["test_table"].columns))
			assert.Equal(t, Column{Name: "new_id", DataType: "INT", PrimaryKey: true}, adapter.adapters["test_table"].columns[0])
		}
	}
	{
		// Adding a primary key
		adapter := initializeAdapter(t)
		{
			// Table does not exist
			err := adapter.ApplyDDL("ALTER TABLE non_existing_table ADD PRIMARY KEY (id);")
			assert.ErrorContains(t, err, `table not found: "non_existing_table"`)
		}
		{
			// Column does not exist
			err := adapter.ApplyDDL("ALTER TABLE test_table ADD PRIMARY KEY (non_existing_column);")
			assert.ErrorContains(t, err, `column not found: "non_existing_column"`)
		}
		{
			// Valid primary key addition
			assert.NoError(t, adapter.ApplyDDL("ALTER TABLE test_table ADD PRIMARY KEY (name);"))
			assert.Equal(t, 3, len(adapter.adapters["test_table"].columns))
			assert.Equal(t, Column{Name: "id", DataType: "INT", PrimaryKey: true}, adapter.adapters["test_table"].columns[0])
			assert.Equal(t, Column{Name: "name", DataType: "VARCHAR(255)", PrimaryKey: true}, adapter.adapters["test_table"].columns[1])
			assert.Equal(t, Column{Name: "email", DataType: "VARCHAR(255)"}, adapter.adapters["test_table"].columns[2])
		}
	}
	{
		// Modifying columns
		adapter := initializeAdapter(t)
		{
			// Table does not exist
			err := adapter.ApplyDDL("ALTER TABLE non_existing_table MODIFY COLUMN id VARCHAR(255);")
			assert.ErrorContains(t, err, `table not found: "non_existing_table"`)
		}
		{
			// Column does not exist
			err := adapter.ApplyDDL("ALTER TABLE test_table MODIFY COLUMN non_existing_column VARCHAR(255);")
			assert.ErrorContains(t, err, `column not found: "non_existing_column"`)
		}
		{
			// Applying one column type change
			assert.NoError(t, adapter.ApplyDDL("ALTER TABLE test_table MODIFY COLUMN id VARCHAR(255);"))
			assert.Equal(t, 3, len(adapter.adapters["test_table"].columns))
			assert.Equal(t, Column{Name: "id", DataType: "VARCHAR(255)", PrimaryKey: true}, adapter.adapters["test_table"].columns[0])
		}
		{
			// Applying multiple column type changes
			assert.NoError(t, adapter.ApplyDDL("ALTER TABLE test_table MODIFY COLUMN id VARCHAR(255), MODIFY COLUMN name INT;"))
			assert.Equal(t, 3, len(adapter.adapters["test_table"].columns))
			assert.Equal(t, Column{Name: "id", DataType: "VARCHAR(255)", PrimaryKey: true}, adapter.adapters["test_table"].columns[0])
			assert.Equal(t, Column{Name: "name", DataType: "INT"}, adapter.adapters["test_table"].columns[1])
		}
	}
	{
		// Dropping columns
		adapter := initializeAdapter(t)
		{
			// Table does not exist
			err := adapter.ApplyDDL("ALTER TABLE non_existing_table DROP COLUMN id;")
			assert.ErrorContains(t, err, `table not found: "non_existing_table"`)
		}
		{
			// Column does not exist
			err := adapter.ApplyDDL("ALTER TABLE test_table DROP COLUMN non_existing_column;")
			assert.ErrorContains(t, err, `column not found: "non_existing_column"`)
		}
		{
			// Dropping one column
			assert.NoError(t, adapter.ApplyDDL("ALTER TABLE test_table DROP COLUMN name;"))
			assert.Equal(t, 2, len(adapter.adapters["test_table"].columns))
			assert.Equal(t, Column{Name: "id", DataType: "INT", PrimaryKey: true}, adapter.adapters["test_table"].columns[0])
			assert.Equal(t, Column{Name: "email", DataType: "VARCHAR(255)"}, adapter.adapters["test_table"].columns[1])
		}
		{
			// Dropping multiple columns
			assert.NoError(t, adapter.ApplyDDL("ALTER TABLE test_table DROP COLUMN id, DROP COLUMN email;"))
			assert.Equal(t, 0, len(adapter.adapters["test_table"].columns))
		}
	}
	{
		// Adding columns
		adapter := initializeAdapter(t)
		{
			// Table does not exist
			err := adapter.ApplyDDL("ALTER TABLE non_existing_table ADD COLUMN id INT;")
			assert.ErrorContains(t, err, `table not found: "non_existing_table"`)
		}
		{
			// Column already exists
			err := adapter.ApplyDDL("ALTER TABLE test_table ADD COLUMN id INT;")
			assert.ErrorContains(t, err, `column already exists: "id"`)
		}
		{
			// Add one column
			assert.NoError(t, adapter.ApplyDDL("ALTER TABLE test_table ADD COLUMN new_column INT;"))
			assert.Equal(t, 4, len(adapter.adapters["test_table"].columns))
			assert.Equal(t, Column{Name: "new_column", DataType: "INT"}, adapter.adapters["test_table"].columns[3])
		}
		{
			// Adding two columns
			assert.NoError(t, adapter.ApplyDDL("ALTER TABLE test_table ADD COLUMN new_column2 INT, ADD COLUMN new_column3 VARCHAR(255);"))
			assert.Equal(t, 6, len(adapter.adapters["test_table"].columns))
			assert.Equal(t, Column{Name: "new_column2", DataType: "INT"}, adapter.adapters["test_table"].columns[4])
			assert.Equal(t, Column{Name: "new_column3", DataType: "VARCHAR(255)"}, adapter.adapters["test_table"].columns[5])
		}
	}
}
