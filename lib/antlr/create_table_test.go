package antlr

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCreateTable(t *testing.T) {
	{
		// Create table LIKE (not currently supported)
		events, err := Parse("CREATE TABLE table_name LIKE other_table;")
		assert.NoError(t, err)
		assert.Len(t, events, 0)
	}
	{
		// Create table with column as CHARACTER SET and collation specified at the column level
		events, err := Parse("CREATE TABLE table_name (id INT, name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci);")
		assert.NoError(t, err)
		assert.Len(t, events, 1)

		createTableEvent, isOk := events[0].(CreateTableEvent)
		assert.True(t, isOk)

		assert.Equal(t, "table_name", createTableEvent.GetTable())
		assert.Len(t, createTableEvent.GetColumns(), 2)
		assert.Equal(t, []Column{{Name: "id", DataType: "INT", PrimaryKey: false}, {Name: "name", DataType: "VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", PrimaryKey: false}}, createTableEvent.GetColumns())
	}
	{
		// Single create table
		for _, tableName := range []string{"table_name", "`table_name`", "db_name.table_name", "`db_name`.`table_name`"} {
			events, err := Parse(fmt.Sprintf("CREATE TABLE %s (id INT, name VARCHAR(255));", tableName))
			assert.NoError(t, err)
			assert.Len(t, events, 1)

			createTableEvent, isOk := events[0].(CreateTableEvent)
			assert.True(t, isOk)

			assert.Equal(t, "table_name", createTableEvent.GetTable())
			assert.Len(t, createTableEvent.GetColumns(), 2)
			assert.Equal(t, []Column{{Name: "id", DataType: "INT", PrimaryKey: false}, {Name: "name", DataType: "VARCHAR(255)", PrimaryKey: false}}, createTableEvent.GetColumns())
		}
	}
	{
		// Create table with primary key
		events, err := Parse("CREATE TABLE table_name (id INT PRIMARY KEY, name VARCHAR(255), tinyint1 TINYINT(1), bool_test BOOLEAN, `escaped_COL` BLOB);")
		assert.NoError(t, err)
		assert.Len(t, events, 1)

		createTableEvent, isOk := events[0].(CreateTableEvent)
		assert.True(t, isOk)

		assert.Equal(t, "table_name", createTableEvent.GetTable())
		assert.Len(t, createTableEvent.GetColumns(), 5)
		assert.Equal(t,
			[]Column{
				{Name: "id", DataType: "INT", PrimaryKey: true},
				{Name: "name", DataType: "VARCHAR(255)", PrimaryKey: false},
				{Name: "tinyint1", DataType: "TINYINT(1)", PrimaryKey: false},
				{Name: "bool_test", DataType: "BOOLEAN", PrimaryKey: false},
				{Name: "escaped_COL", DataType: "BLOB", PrimaryKey: false},
			},
			createTableEvent.GetColumns(),
		)
	}
	{
		events, err := Parse(`create table dt_table(
  dt1 DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  dt2 DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE LOCALTIME,
  dt3 DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE LOCALTIMESTAMP,
  dt4 DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(),
  dt5 DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE LOCALTIME(),
  dt6 DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE LOCALTIMESTAMP(),
  dt7 DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE NOW(),
  dt10 DATETIME DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  dt11 DATETIME DEFAULT '2038-01-01 00:00:00' ON UPDATE CURRENT_TIMESTAMP
);`)
		assert.NoError(t, err)
		assert.Len(t, events, 1)

		createTableEvent, isOk := events[0].(CreateTableEvent)
		assert.True(t, isOk)

		assert.Equal(t, "dt_table", createTableEvent.GetTable())
		assert.Len(t, createTableEvent.GetColumns(), 9)
		assert.Equal(t, []Column{
			{Name: "dt1", DataType: "DATETIME", PrimaryKey: false},
			{Name: "dt2", DataType: "DATETIME", PrimaryKey: false},
			{Name: "dt3", DataType: "DATETIME", PrimaryKey: false},
			{Name: "dt4", DataType: "DATETIME", PrimaryKey: false},
			{Name: "dt5", DataType: "DATETIME", PrimaryKey: false},
			{Name: "dt6", DataType: "DATETIME", PrimaryKey: false},
			{Name: "dt7", DataType: "DATETIME", PrimaryKey: false},
			{Name: "dt10", DataType: "DATETIME", PrimaryKey: false},
			{Name: "dt11", DataType: "DATETIME", PrimaryKey: false},
		}, createTableEvent.GetColumns())
	}
	{
		// Create table (partitioned)
		events, err := Parse(`CREATE TABLE table_items (id INT, purchased DATE)
    PARTITION BY RANGE( YEAR(purchased) )
        SUBPARTITION BY HASH( TO_DAYS(purchased) )
        SUBPARTITIONS 2 (
        PARTITION p0 VALUES LESS THAN (1990),
        PARTITION p1 VALUES LESS THAN (2000),
        PARTITION p2 VALUES LESS THAN MAXVALUE
    );`)
		assert.NoError(t, err)
		assert.Len(t, events, 1)

		createTableEvent, isOk := events[0].(CreateTableEvent)
		assert.True(t, isOk)

		assert.Equal(t, "table_items", createTableEvent.GetTable())
		assert.Len(t, createTableEvent.GetColumns(), 2)
		assert.Equal(t,
			[]Column{{Name: "id", DataType: "INT", PrimaryKey: false}, {Name: "purchased", DataType: "DATE", PrimaryKey: false}},
			createTableEvent.GetColumns(),
		)
	}
	{
		// Create table (VECTOR)
		events, err := Parse("CREATE TABLE TableWithVector (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, vec1 VECTOR, vec2 VECTOR);")
		assert.NoError(t, err)
		assert.Len(t, events, 1)

		createTableEvent, isOk := events[0].(CreateTableEvent)
		assert.True(t, isOk)

		assert.Equal(t, "TableWithVector", createTableEvent.GetTable())
		assert.Len(t, createTableEvent.GetColumns(), 3)
		assert.Equal(t,
			[]Column{
				{Name: "id", DataType: "INT UNSIGNED", PrimaryKey: true},
				{Name: "vec1", DataType: "VECTOR", PrimaryKey: false},
				{Name: "vec2", DataType: "VECTOR", PrimaryKey: false},
			}, createTableEvent.GetColumns())
	}
}
