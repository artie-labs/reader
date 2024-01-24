package constants

type contextKey string

const (
	ConfigKey contextKey = "__cfg"
	KafkaKey  contextKey = "__kafka"
	MtrKey    contextKey = "__mtr"
)

const DefaultLimit = 5_000
