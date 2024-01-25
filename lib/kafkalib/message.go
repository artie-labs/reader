package kafkalib

import (
	"encoding/json"

	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/segmentio/kafka-go"
)

func NewMessage(topic string, key map[string]interface{}, row util.SchemaEventPayload) (kafka.Message, error) {
	jsonBytes, err := json.Marshal(row)
	if err != nil {
		return kafka.Message{}, err
	}

	keyBytes, err := json.Marshal(key)
	if err != nil {
		return kafka.Message{}, err
	}

	return kafka.Message{
		Topic: topic,
		Key:   keyBytes,
		Value: jsonBytes,
	}, nil
}
