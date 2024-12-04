package streaming

import (
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

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

		var parsedAfters []any
		for before, after := range rows {
			assert.Nil(t, before)
			parsedAfters = append(parsedAfters, after)
		}

		assert.Len(t, parsedAfters, 2)
		assert.Equal(t, []any{123, "Dusty", "The Mini Aussie"}, parsedAfters[0])
		assert.Equal(t, []any{456, "Bella", "The Full Size Aussie"}, parsedAfters[1])
	}
}
