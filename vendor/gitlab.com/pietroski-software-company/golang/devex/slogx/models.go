//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate -o fakes . SLogger

package slogx

import (
	"context"
	"log/slog"
)

type (
	SLogger interface {
		SetLogLevel(logLevel slog.Level)

		Info(ctx context.Context, msg string, args ...Attr)
		Debug(ctx context.Context, msg string, args ...Attr)
		Warn(ctx context.Context, msg string, args ...Attr)
		Error(ctx context.Context, msg string, args ...Attr)
		Panic(ctx context.Context, msg string, args ...Attr)
		Fatal(ctx context.Context, msg string, args ...Attr)
	}
)
