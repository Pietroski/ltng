package slogx

import (
	"context"
	"log/slog"
)

type ctxKey string

const (
	slogXFields ctxKey = "slogx_fields"
)

type ContextHandler struct {
	slog.Handler
}

// Handle adds contextual attributes to the Record before calling the underlying handler
func (h *ContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if attrs, ok := ctx.Value(slogXFields).([]slog.Attr); ok {
		for _, v := range attrs {
			r.AddAttrs(v)
		}
	}

	return h.Handler.Handle(ctx, r)
}

// AppendCtx adds an slog attribute to the provided context so that it will be
// included in any Record created with such context
func AppendCtx(ctx context.Context, attr slog.Attr) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	if v, ok := ctx.Value(slogXFields).([]slog.Attr); ok {
		v = append(v, attr)
		return context.WithValue(ctx, slogXFields, v)
	}

	v := make([]slog.Attr, 0)
	v = append(v, attr)
	return context.WithValue(ctx, slogXFields, v)
}
