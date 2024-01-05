package constants

type contextKey string

const (
	ConfigKey contextKey = "__cfg"
	KafkaKey  contextKey = "__kafka"
	LoggerKey contextKey = "__logger"
)
