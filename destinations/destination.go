package destinations

import (
	"context"

	"github.com/artie-labs/reader/lib"
)

type RawMessageIterator interface {
	HasNext() bool
	Next() ([]lib.RawMessage, error)
}

type DestinationWriter interface {
	WriteIterator(ctx context.Context, iter RawMessageIterator) (int, error)
	WriteRawMessages(ctx context.Context, rawMsgs []lib.RawMessage) error
}
