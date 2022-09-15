package badgerdb_manager_controller

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
)

func (c *BadgerDBManagerServiceController) GetStore(
	ctx context.Context,
	req *grpc_mngmt.GetStoreRequest,
) (*grpc_mngmt.GetStoreResponse, error) {
	logger := c.logger.FromCtx(ctx)

	var r management_models.GetStoreRequest
	if err := c.binder.ShouldBind(req, &r); err != nil {
		logger.Errorf(
			"error binding data",
			go_logger.Mapper("err", err.Error()),
		)

		err = status.Error(codes.InvalidArgument, err.Error())
		return &grpc_mngmt.GetStoreResponse{}, err
	}

	dbInfo, err := c.manager.GetDBInfo(ctx, r.Name)
	if err != nil {
		logger.Errorf(
			"error creating or opening database",
			go_logger.Field{
				"err":  err.Error(),
				"name": r.Name,
			},
		)

		err = status.Error(codes.Internal, err.Error())
		return &grpc_mngmt.GetStoreResponse{}, err
	}

	respDbInfo := &grpc_mngmt.DBInfo{
		Name:         dbInfo.Name,
		Path:         dbInfo.Path,
		CreatedAt:    timestamppb.New(dbInfo.CreatedAt),
		LastOpenedAt: timestamppb.New(dbInfo.LastOpenedAt),
	}
	resp := &grpc_mngmt.GetStoreResponse{DbInfo: respDbInfo}

	return resp, nil
}
