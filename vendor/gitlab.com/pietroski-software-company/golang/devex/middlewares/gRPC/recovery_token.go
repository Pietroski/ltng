package gRPC

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	tokenmodels "gitlab.com/pietroski-software-company/golang/devex/tokens/models"
)

const (
	gRPCMetadataRecoveryTokenString = "grpc-metadata-recovery-token-string"

	GGRPCCtxRecoveryTokenString = "ctx-recovery-token-string"
	GRPCCtxRecoveryTokenPayload = "ctx-recovery-token-payload"
)

// GetRecoveryTokenPayloadFromCtx extracts the recovery token payload from the context.
func GetRecoveryTokenPayloadFromCtx(ctx context.Context) *tokenmodels.Payload {
	recoveryTokenPayload, ok := ctx.Value(GRPCCtxRecoveryTokenPayload).(*tokenmodels.Payload)
	if !ok {
		return nil
	}

	return recoveryTokenPayload
}

// GetRecoveryTokenStringFromCtx extracts the recovery token string from the context.
func GetRecoveryTokenStringFromCtx(ctx context.Context) string {
	recoveryTokenString, ok := ctx.Value(GGRPCCtxRecoveryTokenString).(string)
	if !ok {
		return ""
	}

	return recoveryTokenString
}

// RecoveryTokenCtxExtractor extracts the recovery token string and payload from the context.
func RecoveryTokenCtxExtractor(
	ctx context.Context,
	maker tokenmodels.Maker,
) (tokenString string, tokenPayload *tokenmodels.Payload, err error) {
	ctx, err = ServerGRPCRecoveryTokenStringMiddleware(ctx, maker)
	if err != nil {
		return "", nil, fmt.Errorf("failed calling ServerGRPCAuthMiddleware: %v", err)
	}

	tokenPayload = GetRecoveryTokenPayloadFromCtx(ctx)
	if tokenPayload == nil {
		err = fmt.Errorf("no token payload in context")
		return "", nil, fmt.Errorf("failed to get token payload from context: %v", err)
	}

	tokenString = GetRecoveryTokenStringFromCtx(ctx)
	if tokenString == "" {
		err = fmt.Errorf("no token string in context")
		return "", nil, fmt.Errorf("failed to get token string from context: %v", err)
	}

	return tokenString, tokenPayload, nil
}

// RecoveryTokenCtxInjector injects both recovery token string and payload into the context.
func RecoveryTokenCtxInjector(
	ctx context.Context,
	tokenString string,
	tokenPayload *tokenmodels.Payload,
) context.Context {
	ctx = context.WithValue(ctx, GGRPCCtxRecoveryTokenString, tokenString)
	ctx = context.WithValue(ctx, GRPCCtxRecoveryTokenPayload, tokenPayload)

	return ctx
}

// ServerGRPCRecoveryTokenStringMiddleware extracts the metadata info from the incoming request.
// it extracts the raw recovery token string from the incoming metadata.
// it verifies the token and extracts its payload.
// it injects both token string and payload into the context.
// if it fails into any previous step it returns an error.
func ServerGRPCRecoveryTokenStringMiddleware(
	ctx context.Context,
	maker tokenmodels.Maker,
) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, status.Errorf(codes.DataLoss, "failed to get metadata")
	}

	rawToken := md.Get(gRPCMetadataRecoveryTokenString)
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

	ctx = RecoveryTokenCtxInjector(ctx, tokenString, payload)

	return ctx, nil
}

// ClientGRPCRecoveryTokenMiddleware injects the recovery token string from the context into the given metadata.
func ClientGRPCRecoveryTokenMiddleware(
	ctx context.Context,
) (context.Context, error) {
	var md metadata.MD
	md, ctx = CtxMetadataExtractor(ctx)

	tokenString, ok := ctx.Value(GGRPCCtxRecoveryTokenString).(string)
	if !ok {
		return ctx, status.Errorf(codes.DataLoss, "failed to get recovery token string from context")
	}

	md[gRPCMetadataRecoveryTokenString] = []string{tokenString}

	ctx = CtxMetadataInjector(ctx, md)

	return ctx, nil
}
