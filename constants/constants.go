package constants

type contextKey string

const (
	ConfigKey contextKey = "__cfg"
	KafkaKey  contextKey = "__kafka"
)

const (
	DefaultLimit       = 5_000
	DefaultPublishSize = 2_500
)
