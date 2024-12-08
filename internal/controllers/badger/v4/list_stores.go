package badgerdb_controller_v4

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) ListStores(
	ctx context.Context,
	req *grpc_ltngdb.ListStoresRequest,
) (*grpc_ltngdb.ListStoresResponse, error) {
	logger := c.logger.FromCtx(ctx)

	var r badgerdb_management_models_v4.Pagination
	if err := c.binder.ShouldBind(req.Pagination, &r); err != nil {
		logger.Errorf(
			"error binding data",
			go_logger.Mapper("err", err.Error()),
		)

		err = status.Error(codes.InvalidArgument, err.Error())
		return &grpc_ltngdb.ListStoresResponse{}, err
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
		return &grpc_ltngdb.ListStoresResponse{}, err
	}

	return &grpc_ltngdb.ListStoresResponse{
		DbsInfos: dbInfoListRemapper(dbInfoList),
	}, nil
}

func dbInfoListRemapper(
	dbInfoList []*badgerdb_management_models_v4.DBInfo,
) []*grpc_ltngdb.DBInfo {
	dbInfoRespList := make([]*grpc_ltngdb.DBInfo, len(dbInfoList))
	for idx, dbInfo := range dbInfoList {
		if dbInfo == nil {
			dbInfoRespList[idx] = &grpc_ltngdb.DBInfo{}

			continue
		}

		dbInfoResp := &grpc_ltngdb.DBInfo{
			Name:         dbInfo.Name,
			Path:         dbInfo.Path,
			CreatedAt:    timestamppb.New(dbInfo.CreatedAt),
			LastOpenedAt: timestamppb.New(dbInfo.LastOpenedAt),
		}

		dbInfoRespList[idx] = dbInfoResp
	}

	return dbInfoRespList
}
