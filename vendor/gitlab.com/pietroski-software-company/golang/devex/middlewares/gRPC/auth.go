package gRPC

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	tokenmodels "gitlab.com/pietroski-software-company/golang/devex/tokens/models"
)

const (
	gRPCMetadataTokenString = "grpc-metadata-token-string"

	GGRPCCtxTokenString = "ctx-token-string"
	GRPCCtxTokenPayload = "ctx-token-payload"
)

func NewGRPCUnaryAuthClientMiddleware() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) (err error) {
		ctx, err = ClientGRPCAuthMiddleware(ctx)
		if err != nil {
			err = fmt.Errorf("error creating outcoming authentication request: %v", err)
			return err
		}

		// Calls the invoker
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func NewGRPCUnaryAuthServerMiddleware(
	maker tokenmodels.Maker,
) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		ctx, err = ServerGRPCAuthMiddleware(ctx, maker)
		if err != nil {
			err = fmt.Errorf("error authenticating incoming request: %v", err)
			return nil, err
		}

		// Calls the handler
		return handler(ctx, req)
	}
}

// GetTokenPayloadFromCtx extracts the token payload from the context.
func GetTokenPayloadFromCtx(ctx context.Context) *tokenmodels.Payload {
	payload, ok := ctx.Value(GRPCCtxTokenPayload).(*tokenmodels.Payload)
	if !ok {
		return nil
	}

	return payload
}

// GetTokenStringFromCtx extracts the token string from the context.
func GetTokenStringFromCtx(ctx context.Context) string {
	tokenString, ok := ctx.Value(GGRPCCtxTokenString).(string)
	if !ok {
		return ""
	}

	return tokenString
}

// AuthTokenCtxExtractor extracts the token string and payload from the context.
func AuthTokenCtxExtractor(
	ctx context.Context,
	maker tokenmodels.Maker,
) (tokenString string, tokenPayload *tokenmodels.Payload, err error) {
	ctx, err = ServerGRPCAuthMiddleware(ctx, maker)
	if err != nil {
		return "", nil, fmt.Errorf("failed calling ServerGRPCAuthMiddleware: %v", err)
	}

	tokenPayload = GetTokenPayloadFromCtx(ctx)
	if tokenPayload == nil {
		err = fmt.Errorf("no token payload in context")
		return "", nil, fmt.Errorf("failed to get token payload from context: %v", err)
	}

	tokenString = GetTokenStringFromCtx(ctx)
	if tokenString == "" {
		err = fmt.Errorf("no token string in context")
		return "", nil, fmt.Errorf("failed to get token string from context: %v", err)
	}

	return tokenString, tokenPayload, nil
}

// AuthTokenCtxInjector injects both token string and payload into the context.
func AuthTokenCtxInjector(
	ctx context.Context,
	tokenString string,
	tokenPayload *tokenmodels.Payload,
) context.Context {
	ctx = context.WithValue(ctx, GGRPCCtxTokenString, tokenString)
	ctx = context.WithValue(ctx, GRPCCtxTokenPayload, tokenPayload)

	return ctx
}

// ServerGRPCAuthMiddleware extracts the metadata info from the incoming request.
// it extracts the raw token from the incoming metadata.
// it verifies the token and extracts its payload.
// it injects both token string and payload into the context.
// if it fails into any previous step it returns an error.
func ServerGRPCAuthMiddleware(
	ctx context.Context,
	maker tokenmodels.Maker,
) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, status.Errorf(codes.DataLoss, "failed to get metadata")
	}

	rawToken := md.Get(gRPCMetadataTokenString)
	if rawToken == nil ||
		len(rawToken) == 0 ||
		strings.TrimSpace(rawToken[0]) == "" {
		return ctx, status.Errorf(codes.NotFound, "no token found/received")
	}

	tokenString := rawToken[0]
	payload, err := maker.VerifyToken(tokenString)
	if err != nil {
		return ctx, status.Errorf(codes.Unauthenticated, "token not valid: %v", err)
	}

	ctx = AuthTokenCtxInjector(ctx, tokenString, payload)

	return ctx, nil
}

// ClientGRPCAuthMiddleware injects the token string from the context into the given metadata.
func ClientGRPCAuthMiddleware(
	ctx context.Context,
) (context.Context, error) {
	var md metadata.MD
	md, ctx = CtxMetadataExtractor(ctx)

	tokenString, ok := ctx.Value(GGRPCCtxTokenString).(string)
	if !ok {
		return ctx, status.Errorf(codes.DataLoss, "failed to get token string from context")
	}

	md[gRPCMetadataTokenString] = []string{tokenString}

	ctx = CtxMetadataInjector(ctx, md)

	return ctx, nil
}
