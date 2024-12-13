package streaming

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
)

func TestShouldSkipDDL(t *testing.T) {
	{
		// Internal DDL
		assert.True(t, shouldSkipDDL(`BEGIN`))
		assert.True(t, shouldSkipDDL(`INSERT INTO mysql.rds_heartbeat2(id, value) values (1,1734128356976) ON DUPLICATE KEY UPDATE value = 1734128356976`))
	}
	{
		// External DDL
		assert.False(t, shouldSkipDDL(`BEGIN TRANSACTION`))
		assert.False(t, shouldSkipDDL(`CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))`))
		assert.False(t, shouldSkipDDL(`ALTER TABLE users ADD COLUMN id INT`))
	}
}

func TestConvertHeaderToOperation(t *testing.T) {
	{
		// Create
		op, err := convertHeaderToOperation(replication.WRITE_ROWS_EVENTv2)
		assert.NoError(t, err)
		assert.Equal(t, "c", op)
	}
	{
		// Update
		op, err := convertHeaderToOperation(replication.UPDATE_ROWS_EVENTv2)
		assert.NoError(t, err)
		assert.Equal(t, "u", op)
	}
	{
		// Delete
		op, err := convertHeaderToOperation(replication.DELETE_ROWS_EVENTv2)
		assert.NoError(t, err)
		assert.Equal(t, "d", op)
	}

	{
		// Random
		_, err := convertHeaderToOperation(replication.UNKNOWN_EVENT)
		assert.ErrorContains(t, err, "unexpected event type")
	}
}

func TestGetTimeFromEvent(t *testing.T) {
	{
		// nil event
		assert.Equal(t, time.Time{}, getTimeFromEvent(nil))
	}
	{
		// Event is set
		evt := &replication.BinlogEvent{
			Header: &replication.EventHeader{
				Timestamp: uint32(time.Now().Unix()),
			},
		}

		assert.Equal(t, time.Unix(int64(evt.Header.Timestamp), 0), getTimeFromEvent(evt))
	}
}

func TestZipSlicesToMap(t *testing.T) {
	{
		// Invalid
		{
			// More keys than values
			_, err := zipSlicesToMap([]string{"a", "b"}, []any{"c"})
			assert.ErrorContains(t, err, "keys length (2) is different from values length (1)")
		}
		{
			// More values than keys
			_, err := zipSlicesToMap([]string{"a"}, []any{"c", "d"})
			assert.ErrorContains(t, err, "keys length (1) is different from values length (2)")
		}
	}
	{
		// Valid
		{
			// Empty
			out, err := zipSlicesToMap([]string{}, []any{})
			assert.NoError(t, err)
			assert.Equal(t, map[string]any{}, out)
		}
		{
			out, err := zipSlicesToMap([]string{"keyA", "keyB"}, []any{"valueA", "valueB"})
			assert.NoError(t, err)
			assert.Equal(t, map[string]any{"keyA": "valueA", "keyB": "valueB"}, out)
		}
	}
}

func TestSplitIntoBeforeAndAfter(t *testing.T) {
	{
		// Create
		event := [][]any{
			{123, "Dusty", "The Mini Aussie"},
			{456, "Bella", "The Full Size Aussie"},
		}

		rows, err := splitIntoBeforeAndAfter("c", event)
		assert.NoError(t, err)

		var afterList []any
		for before, after := range rows {
			assert.Nil(t, before)
			afterList = append(afterList, after)
		}

		assert.Len(t, afterList, 2)
		assert.Equal(t, []any{123, "Dusty", "The Mini Aussie"}, afterList[0])
		assert.Equal(t, []any{456, "Bella", "The Full Size Aussie"}, afterList[1])
	}
	{
		// Update
		{
			// Invalid - Uneven number of rows.
			event := [][]any{
				{123, "Old Dusty", "The Mini Aussie"},
			}

			_, err := splitIntoBeforeAndAfter("u", event)
			assert.ErrorContains(t, err, "update row count is not divisible by two: 1")
		}
		{
			// Valid
			event := [][]any{
				{123, "Old Dusty", "The Mini Aussie"},
				{123, "New Dusty", "The Mini Aussie"},
				{456, "Old Bella", "The Full Size Aussie"},
				{456, "New Bella", "The Full Size Aussie"},
			}

			rows, err := splitIntoBeforeAndAfter("u", event)
			assert.NoError(t, err)

			var beforeList []any
			var afterList []any
			for before, after := range rows {
				beforeList = append(beforeList, before)
				afterList = append(afterList, after)
			}

			assert.Len(t, beforeList, 2)
			assert.Len(t, afterList, 2)
			{
				// Row 0
				assert.Equal(t, []any{123, "Old Dusty", "The Mini Aussie"}, beforeList[0])
				assert.Equal(t, []any{123, "New Dusty", "The Mini Aussie"}, afterList[0])
			}
			{
				// Row 1
				assert.Equal(t, []any{456, "Old Bella", "The Full Size Aussie"}, beforeList[1])
				assert.Equal(t, []any{456, "New Bella", "The Full Size Aussie"}, afterList[1])
			}
		}
	}
	{
		// Delete
		event := [][]any{
			{123, "Dusty", "The Mini Aussie"},
			{456, "Bella", "The Full Size Aussie"},
		}

		rows, err := splitIntoBeforeAndAfter("d", event)
		assert.NoError(t, err)

		var beforeList []any
		for before, after := range rows {
			beforeList = append(beforeList, before)
			assert.Nil(t, after)
		}

		assert.Len(t, beforeList, 2)
		assert.Equal(t, []any{123, "Dusty", "The Mini Aussie"}, beforeList[0])
		assert.Equal(t, []any{456, "Bella", "The Full Size Aussie"}, beforeList[1])
	}
}
