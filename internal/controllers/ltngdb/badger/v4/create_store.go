package badgerdb_controller_v4

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) CreateStore(
	ctx context.Context,
	req *grpc_ltngdb.CreateStoreRequest,
) (*grpc_ltngdb.CreateStoreResponse, error) {
	payload := &badgerdb_management_models_v4.DBInfo{
		Name:         req.GetName(),
		Path:         req.GetPath(),
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Now(),
	}
	if err := c.manager.CreateStore(ctx, payload); err != nil {
		c.logger.Error(ctx, "error creating or opening database",
			"name", payload.Name,
			"path", payload.Path,
			"created_at", payload.CreatedAt,
			"last_opened_at", payload.LastOpenedAt,
			"error", err,
		)

		err = status.Error(codes.Internal, err.Error())
		return &grpc_ltngdb.CreateStoreResponse{}, err
	}

	return &grpc_ltngdb.CreateStoreResponse{
		CreatedAt:    timestamppb.New(payload.CreatedAt),
		LastOpenedAt: timestamppb.New(payload.LastOpenedAt),
	}, nil
}
