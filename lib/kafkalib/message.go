package kafkalib

import (
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

func newMessage(topic string, partitionKey map[string]any, value any) (kafka.Message, error) {
	valueBytes, err := json.Marshal(value)
	if err != nil {
		return kafka.Message{}, err
	}

	keyBytes, err := json.Marshal(partitionKey)
	if err != nil {
		return kafka.Message{}, err
	}

	return kafka.Message{
		Topic: topic,
		Key:   keyBytes,
		Value: valueBytes,
	}, nil
}
