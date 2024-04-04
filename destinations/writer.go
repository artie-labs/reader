package destinations

import (
	"context"

	"github.com/artie-labs/reader/lib"
)

type DestinationWriter interface {
	WriteRawMessages(ctx context.Context, rawMsgs []lib.RawMessage) error
	OnFinish() error
}
