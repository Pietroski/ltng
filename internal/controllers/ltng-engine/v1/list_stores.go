package ltngdb_controller_v1

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	ltng_engine_models "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltng-engine/v1"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) ListStores(
	ctx context.Context,
	req *grpc_ltngdb.ListStoresRequest,
) (*grpc_ltngdb.ListStoresResponse, error) {
	logger := c.logger.FromCtx(ctx)

	payload := &ltng_engine_models.Pagination{
		PageID:           req.GetPagination().GetPageId(),
		PageSize:         req.GetPagination().GetPageSize(),
		PaginationCursor: req.GetPagination().GetPaginationCursor(),
	}
	infoList, err := c.engine.ListStores(ctx, payload)
	if err != nil {
		logger.Errorf(
			"error creating store",
			go_logger.Field{
				"err": err.Error(),
			},
		)

		err = status.Error(codes.Internal, err.Error())
		return nil, err
	}

	dbInfoList := make([]*grpc_ltngdb.DBInfo, len(infoList))
	for idx, info := range infoList {
		dbInfoList[idx] = &grpc_ltngdb.DBInfo{
			Name:         info.Name,
			Path:         info.Path,
			CreatedAt:    timestamppb.New(time.Unix(info.CreatedAt, 0)),
			LastOpenedAt: timestamppb.New(time.Unix(info.LastOpenedAt, 0)),
		}
	}

	return &grpc_ltngdb.ListStoresResponse{
		DbsInfos: dbInfoList,
	}, nil
}
