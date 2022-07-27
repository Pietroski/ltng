package badgerdb_indexed_manager_controller

import (
	"context"
	"time"

	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
	grpc_indexed_mngmnt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management/indexed/indexed_management"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (c *BadgerDBIndexedManagerServiceController) CreateIndexedStore(
	ctx context.Context,
	req *grpc_indexed_mngmnt.CreateIndexedStoreRequest,
) (*grpc_mngmt.CreateStoreResponse, error) {
	logger := c.logger.FromCtx(ctx)

	var r management_models.CreateStoreRequest
	if err := c.binder.ShouldBind(req, &r); err != nil {
		logger.Errorf(
			"error binding data",
			go_logger.Mapper("err", err.Error()),
		)

		err = status.Error(codes.InvalidArgument, err.Error())
		return &grpc_mngmt.CreateStoreResponse{}, err
	}

	payload := &management_models.DBInfo{
		Name:         r.Name,
		Path:         r.Path,
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Now(),
	}
	if err := c.indexedManager.CreateIndexedStore(ctx, payload); err != nil {
		logger.Errorf(
			"error creating or opening database",
			go_logger.Field{
				"err":            err.Error(),
				"name":           payload.Name,
				"path":           payload.Path,
				"created_at":     payload.CreatedAt,
				"last_opened_at": payload.LastOpenedAt,
			},
		)

		err = status.Error(codes.Internal, err.Error())
		return &grpc_mngmt.CreateStoreResponse{}, err
	}

	return &grpc_mngmt.CreateStoreResponse{
		CreatedAt:    timestamppb.New(payload.CreatedAt),
		LastOpenedAt: timestamppb.New(payload.LastOpenedAt),
	}, nil
}
