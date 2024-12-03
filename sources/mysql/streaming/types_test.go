package streaming

import (
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPosition_UpdatePosition(t *testing.T) {
	pos := Position{File: "file", Pos: 0}
	{
		// Update position with a regular event
		event := &replication.BinlogEvent{
			Header: &replication.EventHeader{
				LogPos:    1234,
				EventType: replication.WRITE_ROWS_EVENTv2,
			},
		}

		assert.NoError(t, pos.UpdatePosition(event))
		assert.Equal(t, uint32(1234), pos.Pos)
		assert.Equal(t, "file", pos.File)
	}
	{
		// Update position with a rotate event
		event := &replication.BinlogEvent{
			Header: &replication.EventHeader{
				LogPos:    888,
				EventType: replication.ROTATE_EVENT,
			},
			Event: &replication.RotateEvent{
				NextLogName: []byte("new_file"),
			},
		}

		assert.NoError(t, pos.UpdatePosition(event))
		assert.Equal(t, uint32(888), pos.Pos)
		assert.Equal(t, "new_file", pos.File)
	}
}
