package badgerdb_manager_controller

import (
	"context"

	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (c *BadgerDBManagerServiceController) DeleteStore(
	ctx context.Context,
	req *grpc_mngmt.DeleteStoreRequest,
) (*grpc_mngmt.DeleteStoreResponse, error) {
	logger := c.logger.FromCtx(ctx)

	var r management_models.DeleteStoreRequest
	if err := c.binder.ShouldBind(req, &r); err != nil {
		logger.Errorf(
			"error binding data",
			go_logger.Mapper("err", err.Error()),
		)

		err = status.Error(codes.InvalidArgument, err.Error())
		return &grpc_mngmt.DeleteStoreResponse{}, err
	}

	if err := c.manager.DeleteFromMemoryAndDisk(ctx, r.Name); err != nil {
		logger.Errorf(
			"error creating or opening database",
			go_logger.Field{
				"err":  err.Error(),
				"name": r.Name,
			},
		)

		err = status.Error(codes.Internal, err.Error())
		return &grpc_mngmt.DeleteStoreResponse{}, err
	}

	return &grpc_mngmt.DeleteStoreResponse{}, nil
}
