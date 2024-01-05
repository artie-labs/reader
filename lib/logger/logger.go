package logger

import (
	"context"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/constants"
	"github.com/evalphobia/logrus_sentry"
	"github.com/sirupsen/logrus"
	"os"
)

func InjectLoggerIntoCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, constants.LoggerKey, initLogger(config.FromContext(ctx)))
}

func FromContext(ctx context.Context) *logrus.Logger {
	logVal := ctx.Value(constants.LoggerKey)
	if logVal == nil {
		return FromContext(InjectLoggerIntoCtx(ctx))
	}

	log, isOk := logVal.(*logrus.Logger)
	if !isOk {
		return FromContext(InjectLoggerIntoCtx(ctx))
	}

	return log
}

func initLogger(settings *config.Settings) *logrus.Logger {
	log := logrus.New()
	log.SetOutput(os.Stdout)

	if settings != nil && settings.Reporting != nil && settings.Reporting.Sentry != nil && settings.Reporting.Sentry.DSN != "" {
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
