package streaming

import (
	"fmt"
	"iter"
	"slices"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
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
func zipSlicesToMap[K comparable, V any](keys []K, values []V) (map[K]V, error) {
	if len(values) != len(keys) {
		return nil, fmt.Errorf("keys length (%d) is different from values length (%d)", len(keys), len(values))
	}

	out := map[K]V{}
	for i, value := range values {
		out[keys[i]] = value
	}
	return out, nil
}

func splitIntoBeforeAndAfter(operation string, rows [][]any) (iter.Seq2[[]any, []any], error) {
	switch operation {
	case "c":
		return func(yield func([]any, []any) bool) {
			for _, row := range rows {
				if !yield(nil, row) {
					return
				}
			}
		}, nil
	case "u":
		// For updates, every modified row is present in the event rows, first as the row before the change and second,
		// as the row after the change.
		// We're assuming that this ordering of rows is consistent.
		if len(rows)%2 != 0 {
			return nil, fmt.Errorf("update row count is not divisible by two: %d", len(rows))
		}

		return func(yield func([]any, []any) bool) {
			for group := range slices.Chunk(rows, 2) {
				if !yield(group[0], group[1]) {
					return
				}
			}
		}, nil
	case "d":
		return func(yield func([]any, []any) bool) {
			for _, row := range rows {
				if !yield(row, nil) {
					return
				}
			}
		}, nil
	default:
		return nil, fmt.Errorf("unsupported operation: %q", operation)
	}
}
