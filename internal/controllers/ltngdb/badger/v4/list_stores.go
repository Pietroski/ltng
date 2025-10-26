package badgerdb_controller_v4

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) ListStores(
	ctx context.Context,
	req *grpc_ltngdb.ListStoresRequest,
) (*grpc_ltngdb.ListStoresResponse, error) {
	dbInfoList, err := c.manager.ListStoreInfo(ctx,
		int(req.GetPagination().GetPageSize()),
		int(req.GetPagination().GetPageId()))
	if err != nil {
		c.logger.Error(ctx, "error listing store info",
			"page_id", req.GetPagination().GetPageId(),
			"page_size", req.GetPagination().GetPageSize(),
			"error", err)

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
