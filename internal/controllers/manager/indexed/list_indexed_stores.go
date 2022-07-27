package badgerdb_indexed_manager_controller

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
	grpc_indexed_mngmnt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management/indexed/indexed_management"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
)

func (c *BadgerDBIndexedManagerServiceController) ListStores(
	ctx context.Context,
	req *grpc_indexed_mngmnt.ListIndexedStoresRequest,
) (*grpc_mngmt.ListStoresResponse, error) {
	logger := c.logger.FromCtx(ctx)

	var r management_models.PaginationRequest
	if err := c.binder.ShouldBind(req.Pagination, &r); err != nil {
		logger.Errorf(
			"error binding data",
			go_logger.Mapper("err", err.Error()),
		)

		err = status.Error(codes.InvalidArgument, err.Error())
		return &grpc_mngmt.ListStoresResponse{}, err
	}

	dbInfoList, err := c.indexedManager.ListIndexedStoreInfo(ctx, int(r.PageSize), int(r.PageID))
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
	dbInfoList []*management_models.DBInfo,
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
