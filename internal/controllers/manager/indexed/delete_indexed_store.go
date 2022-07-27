package badgerdb_indexed_manager_controller

import (
	"context"

	grpc_indexed_mngmnt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management/indexed/indexed_management"

	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (c *BadgerDBIndexedManagerServiceController) DeleteStore(
	ctx context.Context,
	req *grpc_indexed_mngmnt.DeleteIndexedStoreRequest,
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

	if err := c.indexedManager.DeleteIndexedStore(ctx, r.Name); err != nil {
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
