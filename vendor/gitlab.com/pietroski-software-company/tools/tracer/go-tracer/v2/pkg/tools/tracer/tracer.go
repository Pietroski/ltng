package go_tracer

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const (
	CtxTraceStruct = "ctx-trace-struct"

	traceStrFmt = "\n--------------------------------------------------" +
		"\nmessage-trace-id: %v" +
		"\nmessage-trace-created-at: %v" +
		"\nmessage: %s" +
		"\nfields:\n%v" +
		"\n--------------------------------------------------"

	traceStrFmtNoFields = "\n--------------------------------------------------" +
		"\nmessage-trace-id: %v" +
		"\nmessage-trace-created-at: %v" +
		"\nmessage: %s" +
		"\n--------------------------------------------------"
)

type (
	Tracer interface {
		Trace(ctx context.Context) (context.Context, error)
		ResetTrace(ctx context.Context) (context.Context, error)
		Wrap(ctx context.Context, s, f string) (string, error)
		GetTraceInfo(ctx context.Context) (CtxTraceInfo, bool)
	}

	TraceID        uuid.UUID
	TraceCreatedAt time.Time

	CtxTraceInfo struct {
		ID        uuid.UUID
		CreatedAt time.Time
	}

	ctxStructTracer struct{}
)
