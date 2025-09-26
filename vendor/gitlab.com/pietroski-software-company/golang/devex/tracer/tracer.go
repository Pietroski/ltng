package tracer

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const (
	ctxTraceKey = "ctx-trace-key"
)

type (
	Tracer interface {
		Trace(ctx context.Context) (context.Context, error)
		TraceInfo(ctx context.Context) (context.Context, CtxTraceInfo, error)
		ResetTrace(ctx context.Context) (context.Context, error)
		GetTraceInfo(ctx context.Context) (CtxTraceInfo, bool)
		Wrap(ctx context.Context, md Metadata) (context.Context, error)
		Unwrap(ctx context.Context) (Metadata, error)
	}

	CtxTraceInfo struct {
		ID        uuid.UUID
		CreatedAt time.Time
		Metadata  Metadata
	}

	Metadata map[string]any

	ctxStructTracer struct{}
)

// New returns a new Tracer object
func New() Tracer {
	t := &ctxStructTracer{}

	return t
}

// Trace adds tracing to context
func (t *ctxStructTracer) Trace(ctx context.Context) (context.Context, error) {
	return t.checkOrTrace(ctx)
}

// TraceInfo adds tracing to context
func (t *ctxStructTracer) TraceInfo(ctx context.Context) (context.Context, CtxTraceInfo, error) {
	return t.traceInfo(ctx)
}

// ResetTrace resets the trace propagation by overwriting a new tracing into the context.
func (t *ctxStructTracer) ResetTrace(ctx context.Context) (context.Context, error) {
	return t.injectTracing(ctx)
}

// GetTraceInfo returns the trace info from context
func (t *ctxStructTracer) GetTraceInfo(ctx context.Context) (CtxTraceInfo, bool) {
	ctxTraceValue, ok := ctx.Value(ctxTraceKey).(CtxTraceInfo)
	return ctxTraceValue, ok
}

// Wrap wraps the metadata in context
func (t *ctxStructTracer) Wrap(ctx context.Context, md Metadata) (context.Context, error) {
	return t.wrap(ctx, md)
}

// Unwrap unwraps the metadata from context
func (t *ctxStructTracer) Unwrap(ctx context.Context) (Metadata, error) {
	return t.unwrap(ctx)
}
