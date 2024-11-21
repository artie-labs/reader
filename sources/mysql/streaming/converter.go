package streaming

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	"time"
)

func convertHeaderToOperation(evtType replication.EventType) (string, error) {
	switch evtType {
	case replication.WRITE_ROWS_EVENTv2:
		return "c", nil
	case replication.UPDATE_ROWS_EVENTv2:
		return "u", nil
	case replication.DELETE_ROWS_EVENTv2:
		return "d", nil
	default:
		return "", fmt.Errorf("unexpected event type %T", evtType)
	}
}

func getTimeFromEvent(evt *replication.BinlogEvent) time.Time {
	if evt == nil {
		return time.Time{}
	}

	// MySQL binlog only has second precision.
	return time.Unix(int64(evt.Header.Timestamp), 0)
}

// zipSlicesToMap creates a map from two slices, one of keys and one of values.
func zipSlicesToMap(keys []string, values []any) (map[string]any, error) {
	if len(values) != len(keys) {
		return nil, fmt.Errorf("keys length (%d) is different from values length (%d)", len(keys), len(values))
	}

	out := map[string]any{}
	for i, value := range values {
		out[keys[i]] = value
	}
	return out, nil
}
