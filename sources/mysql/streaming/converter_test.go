package streaming

import (
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
	"testing"
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
