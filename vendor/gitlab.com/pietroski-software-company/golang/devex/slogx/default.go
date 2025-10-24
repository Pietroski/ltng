package slogx

import (
	"log/slog"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/tracer"
)

func defaultLogger(opts ...Option) *Slog {
	logLevel := &slog.LevelVar{} // default INFO

	defaultOpts := &slog.HandlerOptions{
		AddSource:   false, // if true, it shows the correct path the log functions from Slog are being called.
		Level:       logLevel,
		ReplaceAttr: defaultReplaceAttrs(),
	}
	ApplyOptions(defaultOpts, opts...)

	handler := slog.NewJSONHandler(os.Stdout, defaultOpts)
	ctxHandler := &ContextHandler{handler}
	tracingHandler := &TraceHandler{
		Handler: ctxHandler,
		tracer:  tracer.New(),
	}
	logger := slog.New(tracingHandler)

	l := &Slog{
		initialLogLevel: defaultOpts.Level.Level(),
		logger:          logger,
		logLevel:        logLevel,
		tracer:          tracer.New(),
		osExit:          os.Exit,
	}
	l.setDefaultTimer()
	ApplySlogOptions(l, opts...)

	return l
}

var defaultLogLevel = &slog.LevelVar{}

func DefaultLogger(opts ...Option) *Slog {
	return defaultLogger(append(opts, WithLogLevel(defaultLogLevel.Level()))...)
}

func SetDefaultLogLevel(level slog.Level) {
	defaultLogLevel.Set(level)
}
