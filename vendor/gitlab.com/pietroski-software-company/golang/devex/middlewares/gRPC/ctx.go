package gRPC

import (
	"context"

	"google.golang.org/grpc/metadata"
)

func CtxMetadataInversor(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		ctx = metadata.NewOutgoingContext(ctx, md)
		return ctx
	}

	md, ok = metadata.FromOutgoingContext(ctx)
	if ok {
		ctx = metadata.NewIncomingContext(ctx, md)
		return ctx
	}

	return ctx
}

func CtxMetadataInjector(
	ctx context.Context,
	md metadata.MD,
) context.Context {
	ctx = metadata.NewIncomingContext(ctx, md)
	ctx = metadata.NewOutgoingContext(ctx, md)

	return ctx
}

func CtxMetadataExtractor(ctx context.Context) (metadata.MD, context.Context) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		return md, ctx
	}

	md, ok = metadata.FromOutgoingContext(ctx)
	if ok {
		return md, ctx
	}

	return metadata.MD{}, ctx
}

func CtxIncomingMetadataExtractor(ctx context.Context) (metadata.MD, context.Context) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		return md, ctx
	}

	return metadata.MD{}, ctx
}

func CtxOutgoingMetadataExtractor(ctx context.Context) (metadata.MD, context.Context) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if ok {
		return md, ctx
	}

	return metadata.MD{}, ctx
}
