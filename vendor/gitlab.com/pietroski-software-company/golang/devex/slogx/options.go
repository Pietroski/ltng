package slogx

import (
	"log/slog"
)

// Option defines the function signature for configuration options.
type Option func(cfg interface{})

// ApplyOptions applies a list of options to a configuration.
func ApplyOptions(cfg *slog.HandlerOptions, opts ...Option) {
	for _, opt := range opts {
		opt(cfg)
	}
}

// WithSource sets the slog.HandlerOptions.AddSource on slogx initialization.
func WithSource(addSrc bool) Option {
	return func(cfg interface{}) {
		if c, ok := cfg.(*slog.HandlerOptions); ok {
			c.AddSource = addSrc
		}
	}
}

// WithLogLevel sets the log level at the logger initialization.
func WithLogLevel(logLevel slog.Level) Option {
	return func(cfg interface{}) {
		if c, ok := cfg.(*slog.HandlerOptions); ok {
			ll := &slog.LevelVar{}
			ll.Set(logLevel)
			c.Level = ll
		}
	}
}

// WithExtraSlogReplaceAttrs set extra slog replace attributes.
func WithExtraSlogReplaceAttrs(extraAttr ...SlogFnAttr) Option {
	return func(cfg interface{}) {
		if c, ok := cfg.(*slog.HandlerOptions); ok {
			c.ReplaceAttr = extraReplaceAttrs(extraAttr...)
		}
	}
}

func WithLogLevelTTL(duration int) Option {
	return func(cfg interface{}) {
		if c, ok := cfg.(*Slog); ok {
			c.logLevelTTL.Store(uint64(duration))
		}
	}
}

func ApplySlogOptions(cfg *Slog, opts ...Option) {
	for _, opt := range opts {
		opt(cfg)
	}
}

// WithSLogLevel sets the log level at the logger initialization.
func WithSLogLevel(logLevel slog.Level) Option {
	return func(cfg interface{}) {
		if c, ok := cfg.(*Slog); ok {
			c.initialLogLevel = logLevel
			c.SetLogLevel(logLevel)
		}
	}
}
