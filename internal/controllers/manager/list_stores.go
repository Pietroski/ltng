package badgerdb_manager_controller

import (
	"context"
	go_tracer "gitlab.com/pietroski-software-company/tools/tracer/go-tracer/v2/pkg/tools/tracer"
	"google.golang.org/protobuf/types/known/timestamppb"

	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
)

func (c *BadgerDBManagerServiceController) ListStores(
	ctx context.Context,
	req *grpc_mngmt.ListStoresRequest,
) (*grpc_mngmt.ListStoresResponse, error) {
	logger := c.logger.FromCtx(ctx)

	{
		logger.Debugf("inside list stores")

		ctxT, ok := go_tracer.NewCtxTracer().GetTraceInfo(ctx)
		logger.Debugf(
			"tracing info inside list handler",
			go_logger.Field{
				"ok":   ok,
				"ctxT": ctxT,
			},
		)
	}

	var r management_models.PaginationRequest
	if err := c.binder.ShouldBind(req.Pagination, &r); err != nil {
		logger.Errorf(
			"error binding data",
			go_logger.Mapper("err", err.Error()),
		)

		err = status.Error(codes.InvalidArgument, err.Error())
		return &grpc_mngmt.ListStoresResponse{}, err
	}

	dbInfoList, err := c.manager.ListStoreInfoFromMemoryOrDisk(ctx, int(r.PageSize), int(r.PageID))
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
