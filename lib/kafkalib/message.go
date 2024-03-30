package kafkalib

import (
	"encoding/json"
	"fmt"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/segmentio/kafka-go"
)

func newMessage(cfg config.Kafka, rawMessage lib.RawMessage) (kafka.Message, error) {
	valueBytes, err := json.Marshal(rawMessage.GetPayload())
	if err != nil {
		return kafka.Message{}, err
	}

	keyBytes, err := json.Marshal(rawMessage.PartitionKey)
	if err != nil {
		return kafka.Message{}, err
	}

	return kafka.Message{
		Topic: fmt.Sprintf("%s.%s", cfg.TopicPrefix, rawMessage.TopicSuffix),
		Key:   keyBytes,
		Value: valueBytes,
	}, nil
}
