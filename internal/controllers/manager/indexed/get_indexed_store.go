package badgerdb_indexed_manager_controller

import (
	"context"

	grpc_indexed_mngmnt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management/indexed/indexed_management"

	"google.golang.org/protobuf/types/known/timestamppb"

	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
)

func (c *BadgerDBIndexedManagerServiceController) GetStore(
	ctx context.Context,
	req *grpc_indexed_mngmnt.GetIndexedStoreRequest,
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

	dbInfo, err := c.indexedManager.GetDBInfo(ctx, r.Name)
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
