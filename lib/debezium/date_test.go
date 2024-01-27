package debezium

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestToDebeziumDate(t *testing.T) {
	ts := time.Date(2023, 5, 3, 0, 0, 0, 0, time.UTC)
	days, err := ToDebeziumDate(ts)
	assert.NoError(t, err)
	assert.Equal(t, 19480, days)
}
