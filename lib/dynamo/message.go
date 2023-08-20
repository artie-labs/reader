package dynamo

import (
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

type Message struct {
	*dynamodbstreams.Record
}

func NewMessage(record *dynamodbstreams.Record) *Message {
	return &Message{
		Record: record,
	}
}

func (m *Message) toArtieMessage() (util.SchemaEventPayload, error) {
	return util.SchemaEventPayload{}, nil
}

func (m *Message) Publish() error {
	return nil
}

func (m *Message) operation() string {
	switch *m.EventName {
	case "INSERT":
		return "c"
	case "MODIFY":
		return "u"
	case "REMOVE":
		return "d"
	}

	return "r"
}
