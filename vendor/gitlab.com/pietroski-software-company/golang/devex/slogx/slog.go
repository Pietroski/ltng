package slogx

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"

	"gitlab.com/pietroski-software-company/golang/devex/tracer"
)

type Slog struct {
	logger          *slog.Logger
	logLevel        *slog.LevelVar
	initialLogLevel slog.Level

	logLevelTTL    *atomic.Uint64
	hasSetLogLevel *atomic.Bool
	tracer         tracer.Tracer

	osExit func(int)
}

// New instantiates a new Slog type.
func New(opts ...Option) *Slog {
	return defaultLogger(opts...)
}

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
		initialLogLevel: logLevel.Level(),
		logger:          logger,
		logLevel:        logLevel,
		tracer:          tracer.New(),
		osExit:          os.Exit,
	}
	l.setDefaultTimer()
	ApplySlogOptions(l, opts...)

	return l
}

// SetLogLevel sets the log level during the runtime
func (l *Slog) SetLogLevel(logLevel slog.Level) {
	l.logLevel.Set(logLevel)
	l.setLogLevelReset()
}

// Info logs info level logs
func (l *Slog) Info(ctx context.Context, msg string, args ...Attr) {
	l.logger.LogAttrs(ctx, slog.LevelInfo, msg, processAttrs(args)...)
}

// Debug logs debug level logs
func (l *Slog) Debug(ctx context.Context, msg string, args ...Attr) {
	l.logger.LogAttrs(ctx, slog.LevelDebug, msg, processAttrs(args)...)
}

// Error logs error level logs
func (l *Slog) Error(ctx context.Context, msg string, args ...Attr) {
	l.logger.LogAttrs(ctx, slog.LevelError, msg, processAttrs(args)...)
}

// Warn logs warn level logs
func (l *Slog) Warn(ctx context.Context, msg string, args ...Attr) {
	l.logger.LogAttrs(ctx, slog.LevelWarn, msg, processAttrs(args)...)
}

// Trace logs trace level logs
func (l *Slog) Trace(ctx context.Context, msg string, args ...Attr) {
	l.logger.LogAttrs(ctx, LevelTrace, msg, processAttrs(args)...)
}

// Test logs Test level logs
func (l *Slog) Test(ctx context.Context, msg string, args ...Attr) {
	l.logger.LogAttrs(ctx, LevelTest, msg, processAttrs(args)...)
}

// Panic logs panic level logs
func (l *Slog) Panic(ctx context.Context, msg string, args ...Attr) {
	l.logger.LogAttrs(ctx, LevelFatal, msg, processAttrs(args)...)
	panic(msg)
}

// Fatal logs fatal level logs
func (l *Slog) Fatal(ctx context.Context, msg string, args ...Attr) {
	l.logger.LogAttrs(ctx, LevelFatal, msg, processAttrs(args)...)
	l.osExit(1)
}

var _ SLogger = &Slog{}
var _ SLogger = (*Slog)(nil)
