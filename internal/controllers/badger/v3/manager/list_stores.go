package badgerdb_manager_controller_v3

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	badgerdb_management_models_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v3/management"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
)

func (c *BadgerDBManagerServiceControllerV3) ListStores(
	ctx context.Context,
	req *grpc_mngmt.ListStoresRequest,
) (*grpc_mngmt.ListStoresResponse, error) {
	logger := c.logger.FromCtx(ctx)

	var r badgerdb_management_models_v3.Pagination
	if err := c.binder.ShouldBind(req.Pagination, &r); err != nil {
		logger.Errorf(
			"error binding data",
			go_logger.Mapper("err", err.Error()),
		)

		err = status.Error(codes.InvalidArgument, err.Error())
		return &grpc_mngmt.ListStoresResponse{}, err
	}

	dbInfoList, err := c.manager.ListStoreInfo(ctx, int(r.PageSize), int(r.PageID))
	if err != nil {
		logger.Errorf(
			"error creating or opening database",
			go_logger.Field{
				"err":       err.Error(),
				"page_id":   &r.PageID,
				"page_size": &r.PageSize,
			},
		)

		err = status.Error(codes.Internal, err.Error())
		return &grpc_mngmt.ListStoresResponse{}, err
	}

	return &grpc_mngmt.ListStoresResponse{
		DbsInfos: dbInfoListRemapper(dbInfoList),
	}, nil
}

func dbInfoListRemapper(
	dbInfoList []*badgerdb_management_models_v3.DBInfo,
) []*grpc_mngmt.DBInfo {
	dbInfoRespList := make([]*grpc_mngmt.DBInfo, len(dbInfoList))
	for idx, dbInfo := range dbInfoList {
		if dbInfo == nil {
			dbInfoRespList[idx] = &grpc_mngmt.DBInfo{}

			continue
		}

		dbInfoResp := &grpc_mngmt.DBInfo{
			Name:         dbInfo.Name,
			Path:         dbInfo.Path,
			CreatedAt:    timestamppb.New(dbInfo.CreatedAt),
			LastOpenedAt: timestamppb.New(dbInfo.LastOpenedAt),
		}

		dbInfoRespList[idx] = dbInfoResp
	}

	return dbInfoRespList
}
