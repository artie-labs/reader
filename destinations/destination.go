package destinations

import (
	"context"

	"github.com/artie-labs/reader/lib"
)

type Destination interface {
	WriteRawMessages(ctx context.Context, rawMsgs []lib.RawMessage) error
}
