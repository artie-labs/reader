package constants

type contextKey string

const (
	ConfigKey contextKey = "__cfg"
)

const (
	DefaultLimit       = 5_000
	DefaultPublishSize = 2_500
)
