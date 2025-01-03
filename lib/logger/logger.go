package logger

import (
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
	slogmulti "github.com/samber/slog-multi"
	slogsentry "github.com/samber/slog-sentry/v2"

	"github.com/artie-labs/reader/config"
)

var handlersToTerminate []func()

func NewLogger(settings *config.Settings) (*slog.Logger, func()) {
	logLevel := slog.LevelInfo
	if val := os.Getenv("READER_DEBUG"); val != "" {
		debug, err := strconv.ParseBool(val)
		if err != nil {
			Panic("Failed to parse READER_DEBUG", slog.Any("err", err))
		}

		if debug {
			logLevel = slog.LevelDebug
		}
	}

	handler := tint.NewHandler(os.Stderr, &tint.Options{
		Level:   logLevel,
		NoColor: !isatty.IsTerminal(os.Stderr.Fd()),
	})

	if settings != nil && settings.Reporting != nil && settings.Reporting.Sentry != nil && settings.Reporting.Sentry.DSN != "" {
		if err := sentry.Init(sentry.ClientOptions{Dsn: settings.Reporting.Sentry.DSN}); err != nil {
			slog.New(handler).Warn("Failed to enable Sentry output", slog.Any("err", err))
		} else {
			handler = slogmulti.Fanout(
				handler,
				slogsentry.Option{Level: slog.LevelError}.NewSentryHandler(),
			)

			slog.New(handler).Info("Sentry logger enabled")
			handlersToTerminate = append(handlersToTerminate, func() {
				sentry.Flush(2 * time.Second)
			})
		}
	}

	return slog.New(handler), runHandlers
}

func runHandlers() {
	for _, handlerToTerminate := range handlersToTerminate {
		handlerToTerminate()
	}
}

func Fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	runHandlers()
	os.Exit(1)
}

func Panic(msg string, args ...any) {
	slog.Error(msg, args...)
	runHandlers()
	panic(msg)
}
