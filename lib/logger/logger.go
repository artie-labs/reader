package logger

import (
	"context"
	"github.com/artie-labs/reader/config"
	"github.com/evalphobia/logrus_sentry"
	"github.com/sirupsen/logrus"
	"os"
)

const loggerKey = "_log"

func InjectLoggerIntoCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, loggerKey, new(config.FromContext(ctx)))
}

func FromContext(ctx context.Context) *logrus.Logger {
	logVal := ctx.Value(loggerKey)
	if logVal == nil {
		// Inject this back into context, so we don't need to initialize this again
		return FromContext(InjectLoggerIntoCtx(ctx))
	}

	log, isOk := logVal.(*logrus.Logger)
	if !isOk {
		return FromContext(InjectLoggerIntoCtx(ctx))
	}

	return log
}

func new(settings *config.Settings) *logrus.Logger {
	log := logrus.New()
	log.SetOutput(os.Stdout)

	if settings != nil && settings.Reporting.Sentry != nil && settings.Reporting.Sentry.DSN != "" {
		hook, err := logrus_sentry.NewSentryHook(settings.Reporting.Sentry.DSN, []logrus.Level{
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
			logrus.WarnLevel,
		})

		if err != nil {
			log.WithError(err).Warn("Failed to enable Sentry output")
		} else {
			log.Hooks.Add(hook)
		}
	}

	return log
}
