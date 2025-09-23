package slogx

import (
	"context"
	"log/slog"

	"gitlab.com/pietroski-software-company/golang/devex/tracer"
)

const traceIDKey = "trace_id"

type TraceHandler struct {
	slog.Handler
	tracer tracer.Tracer
}

// Handle adds trace attributes to the Record before calling the underlying handler
func (h *TraceHandler) Handle(ctx context.Context, r slog.Record) error {
	ctx, ctxTraceInfo, err := h.tracer.TraceInfo(ctx)
	if err != nil {
		return err
	}

	r.AddAttrs(
		slog.Attr{
			Key:   traceIDKey,
			Value: slog.StringValue(ctxTraceInfo.ID.String()),
		},
	)

	return h.Handler.Handle(ctx, r)
}
