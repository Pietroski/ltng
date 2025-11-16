package ltngdbcontrollerv3

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) ListStores(
	ctx context.Context,
	req *grpc_ltngdb.ListStoresRequest,
) (*grpc_ltngdb.ListStoresResponse, error) {
	payload := &ltngdata.Pagination{
		PageID:           req.GetPagination().GetPageId(),
		PageSize:         req.GetPagination().GetPageSize(),
		PaginationCursor: req.GetPagination().GetPaginationCursor(),
	}
	infoList, err := c.engine.ListStores(ctx, payload)
	if err != nil {
		c.logger.Error(ctx, "error listing store", "error", err)

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
