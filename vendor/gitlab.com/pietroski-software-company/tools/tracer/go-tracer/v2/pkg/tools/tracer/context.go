package go_tracer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
)

func NewCtxTracing() (CtxTraceInfo, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return CtxTraceInfo{}, fmt.Errorf("error to generate new uuid: %v", err)
	}

	return CtxTracingFromUUID(id), nil
}

func CtxTracingFromUUID(UUID uuid.UUID) CtxTraceInfo {
	return CtxTraceInfo{
		ID:        UUID,
		CreatedAt: time.Now(),
	}
}

func NewCtxTracer() Tracer {
	t := &ctxStructTracer{}

	return t
}

// Trace traces
func (t *ctxStructTracer) Trace(ctx context.Context) (context.Context, error) {
	return t.ctxTracePipeline(ctx)
}

// ResetTrace resets the trace propagation
func (t *ctxStructTracer) ResetTrace(ctx context.Context) (context.Context, error) {
	return t.ctxResetTrace(ctx)
}

func (t *ctxStructTracer) Wrap(ctx context.Context, s, f string) (string, error) {
	return t.ctxStrWrapper(ctx, s, f)
}

func (t *ctxStructTracer) GetTraceInfo(ctx context.Context) (CtxTraceInfo, bool) {
	return t.checkForTraceStruct(ctx)
}

func (t *ctxStructTracer) ctxTracePipeline(ctx context.Context) (context.Context, error) {
	_, ok := t.checkForTraceStruct(ctx)
	if !ok {
		var err error
		ctx, err = t.injectNewTraceStruct(ctx)
		if err != nil {
			return ctx, fmt.Errorf("failed to create ctxtracing: %v", err)
		}
	}

	return ctx, nil
}

func (t *ctxStructTracer) checkForTraceStruct(ctx context.Context) (CtxTraceInfo, bool) {
	ctxTraceValue, ok := ctx.Value(CtxTraceStruct).(CtxTraceInfo)

	return ctxTraceValue, ok
}

func (t *ctxStructTracer) injectNewTraceStruct(ctx context.Context) (context.Context, error) {
	ctxT, err := NewCtxTracing()
	if err != nil {
		return ctx, fmt.Errorf("failed to create ctxtracing: %v", err)
	}
	ctx = context.WithValue(ctx, CtxTraceStruct, ctxT)

	return ctx, nil
}

func (t *ctxStructTracer) ctxResetTrace(ctx context.Context) (context.Context, error) {
	return t.injectNewTraceStruct(ctx)
}

func (t *ctxStructTracer) ctxStrWrapper(ctx context.Context, s, f string) (string, error) {
	ct, ok := t.checkForTraceStruct(ctx)
	if !ok {
		log.Println("given context dit not have any tracers before")
		log.Println("creating new tracing event...")
		ct, err := NewCtxTracing()
		if err != nil {
			return "", fmt.Errorf("failed to create ctxtracing: %v", err)
		}
		log.Printf("trace-id: %v\ncreated-at: %v\n", ct.ID.String(), ct.CreatedAt.String())
	}

	var str string
	if f == "{}" {
		str = fmt.Sprintf(traceStrFmtNoFields, ct.ID.String(), ct.CreatedAt.String(), s)
	} else {
		str = fmt.Sprintf(traceStrFmt, ct.ID.String(), ct.CreatedAt.String(), s, f)
	}

	return str, nil
}
