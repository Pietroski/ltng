package tracer

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

func NewCtxTracingInfo() (CtxTraceInfo, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return CtxTraceInfo{}, fmt.Errorf("error to generate new uuid: %v", err)
	}

	return CtxTraceInfo{
		ID:        id,
		CreatedAt: time.Now(),
		Metadata:  make(map[string]any),
	}, nil
}

func (t *ctxStructTracer) checkOrTrace(ctx context.Context) (context.Context, error) {
	_, ok := ctx.Value(ctxTraceKey).(CtxTraceInfo)
	if !ok {
		var err error
		ctx, err = t.injectTracing(ctx)
		if err != nil {
			return ctx, fmt.Errorf("failed to create ctxtracing: %v", err)
		}
	}

	return ctx, nil
}

func (t *ctxStructTracer) traceInfo(ctx context.Context) (context.Context, CtxTraceInfo, error) {
	traceInfo, ok := ctx.Value(ctxTraceKey).(CtxTraceInfo)
	if !ok {
		var err error
		ctx, traceInfo, err = t.injectAndReturn(ctx)
		if err != nil {
			return ctx, CtxTraceInfo{}, fmt.Errorf("failed to create ctxtracing: %v", err)
		}
	}

	return ctx, traceInfo, nil
}

func (t *ctxStructTracer) injectAndReturn(ctx context.Context) (context.Context, CtxTraceInfo, error) {
	ctxTracingInfo, err := NewCtxTracingInfo()
	if err != nil {
		return ctx, CtxTraceInfo{}, fmt.Errorf("failed to create ctxtracing: %v", err)
	}
	ctx = context.WithValue(ctx, ctxTraceKey, ctxTracingInfo)

	return ctx, ctxTracingInfo, nil
}

func (t *ctxStructTracer) injectTracing(ctx context.Context) (context.Context, error) {
	ctxTracingInfo, err := NewCtxTracingInfo()
	if err != nil {
		return ctx, fmt.Errorf("failed to create ctxtracing: %v", err)
	}
	ctx = context.WithValue(ctx, ctxTraceKey, ctxTracingInfo)

	return ctx, nil
}

func (t *ctxStructTracer) wrap(ctx context.Context, md Metadata) (context.Context, error) {
	ctxTraceValue, ok := ctx.Value(ctxTraceKey).(CtxTraceInfo)
	if !ok {
		var err error
		ctxTraceValue, err = NewCtxTracingInfo()
		if err != nil {
			return ctx, fmt.Errorf("failed to create ctx tracing info: %w", err)
		}
	}

	for k, v := range md {
		ctxTraceValue.Metadata[k] = v
	}

	ctx = context.WithValue(ctx, ctxTraceKey, ctxTraceValue)
	return ctx, nil
}

func (t *ctxStructTracer) unwrap(ctx context.Context) (Metadata, error) {
	ctxTraceValue, ok := ctx.Value(ctxTraceKey).(CtxTraceInfo)
	if !ok {
		var err error
		ctxTraceValue, err = NewCtxTracingInfo()
		if err != nil {
			return Metadata{}, fmt.Errorf("failed to create ctx tracing info: %w", err)
		}
	}

	return ctxTraceValue.Metadata, nil
}
