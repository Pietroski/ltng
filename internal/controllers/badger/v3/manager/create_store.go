package badgerdb_manager_controller_v3

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	badgerdb_management_models_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v3/management"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
)

func (c *BadgerDBManagerServiceControllerV3) CreateStore(
	ctx context.Context,
	req *grpc_mngmt.CreateStoreRequest,
) (*grpc_mngmt.CreateStoreResponse, error) {
	logger := c.logger.FromCtx(ctx)

	var r badgerdb_management_models_v3.CreateStoreRequest
	if err := c.binder.ShouldBind(req, &r); err != nil {
		logger.Errorf(
			"error binding data",
			go_logger.Mapper("err", err.Error()),
		)

		err = status.Error(codes.InvalidArgument, err.Error())
		return &grpc_mngmt.CreateStoreResponse{}, err
	}

	payload := &badgerdb_management_models_v3.DBInfo{
		Name:         r.Name,
		Path:         r.Path,
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Now(),
	}
	if err := c.manager.CreateStore(ctx, payload); err != nil {
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
