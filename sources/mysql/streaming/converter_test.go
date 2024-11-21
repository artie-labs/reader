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
