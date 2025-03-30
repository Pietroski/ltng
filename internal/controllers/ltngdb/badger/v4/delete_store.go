package badgerdb_controller_v4

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) DeleteStore(
	ctx context.Context,
	req *grpc_ltngdb.DeleteStoreRequest,
) (*grpc_ltngdb.DeleteStoreResponse, error) {
	logger := c.logger.FromCtx(ctx)

	var r badgerdb_management_models_v4.DeleteStoreRequest
	if err := c.binder.ShouldBind(req, &r); err != nil {
		logger.Errorf(
			"error binding data",
			go_logger.Mapper("err", err.Error()),
		)

		err = status.Error(codes.InvalidArgument, err.Error())
		return &grpc_ltngdb.DeleteStoreResponse{}, err
	}

	if err := c.manager.DeleteStore(ctx, r.Name); err != nil {
		logger.Errorf(
			"error creating or opening database",
			go_logger.Field{
				"err":  err.Error(),
				"name": r.Name,
			},
		)

		err = status.Error(codes.Internal, err.Error())
		return &grpc_ltngdb.DeleteStoreResponse{}, err
	}

	return &grpc_ltngdb.DeleteStoreResponse{}, nil
}
