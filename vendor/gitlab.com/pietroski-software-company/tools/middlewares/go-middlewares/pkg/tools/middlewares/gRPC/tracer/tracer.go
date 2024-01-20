package go_grpc_tracer_middleware

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"strings"

	go_tracer "gitlab.com/pietroski-software-company/tools/tracer/go-tracer/v2/pkg/tools/tracer"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	gRPCTraceID = "grpc-metadata-trace-id"
)

func NewGRPCUnaryTracerClientMiddleware(
	tracer go_tracer.Tracer,
) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) (err error) {
		md := metadata.MD{}
		ctx, md, err = GRPCClientTracer(ctx, md, tracer)
		if err != nil {
			err = fmt.Errorf("error tracing outcoming request: %v", err)
			return err
		}

		ctx = metadata.NewOutgoingContext(ctx, md)

		// Calls the invoker
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func NewGRPCUnaryTracerServerMiddleware() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		ctx, err = GRPCServerTracer(ctx)
		if err != nil {
			err = fmt.Errorf("error tracing incoming request: %v", err)
			return nil, err
		}

		// Calls the handler
		return handler(ctx, req)
	}
}

func GRPCServerTracer(
	ctx context.Context,
) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, status.Errorf(codes.DataLoss, "failed to get metadata")
	}

	rawValue := md.Get(gRPCTraceID)
	if rawValue == nil || len(rawValue) == 0 {
		rawValue = []string{""}
	}
	if strings.TrimSpace(rawValue[0]) == "" {
		UUID, err := uuid.NewRandom()
		if err != nil {
			return ctx, status.Errorf(codes.Internal, "failed to create new uuid for context tracing")
		}

		rawValue[0] = UUID.String()
	}

	UUID, err := uuid.Parse(rawValue[0])
	if err != nil {
		return ctx, status.Errorf(codes.Internal, "failed to parse uuid for context tracing")
	}

	ctxT := go_tracer.CtxTracingFromUUID(UUID)
	ctx = context.WithValue(ctx, go_tracer.CtxTraceStruct, ctxT)

	return go_tracer.NewCtxTracer().Trace(ctx)
}

func GRPCClientTracer(
	ctx context.Context,
	md metadata.MD,
	t go_tracer.Tracer,
) (context.Context, metadata.MD, error) {
	ctxT, ok := t.GetTraceInfo(ctx)
	if !ok {
		ctx, err := t.Trace(ctx)
		if err != nil {
			const str = "failed to get the trace info from context"
			err = fmt.Errorf("%s: failed to trace context: %v", str, err)
			return ctx, md, err
		}

		md[gRPCTraceID] = []string{ctxT.ID.String()}

		return ctx, md, nil
	}

	md[gRPCTraceID] = []string{ctxT.ID.String()}

	return ctx, md, nil
}
