package ltngdbcontrollerv3

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ltngdbenginemodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	grpc_pagination "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/common/search"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) List(
	ctx context.Context,
	req *grpc_ltngdb.ListRequest,
) (*grpc_ltngdb.ListResponse, error) {
	dbMetaInfo := &ltngdbenginemodelsv3.ManagerStoreMetaInfo{
		Name: req.GetDatabaseMetaInfo().GetDatabaseName(),
		Path: req.GetDatabaseMetaInfo().GetDatabasePath(),
	}
	pagination := &ltngdata.Pagination{
		PageID:           req.GetPagination().GetPageId(),
		PageSize:         req.GetPagination().GetPageSize(),
		PaginationCursor: req.GetPagination().GetPaginationCursor(),
	}
	opts := &ltngdbenginemodelsv3.IndexOpts{
		HasIdx:       req.GetIndexOpts().GetHasIdx(),
		ParentKey:    req.GetIndexOpts().GetParentKey(),
		IndexingKeys: req.GetIndexOpts().GetIndexingKeys(),
		IndexProperties: ltngdbenginemodelsv3.IndexProperties{
			ListSearchPattern: ltngdbenginemodelsv3.ListSearchPattern(
				req.GetIndexOpts().GetIndexingProperties().GetListSearchPattern(),
			),
		},
	}
	loadedItems, err := c.engine.ListItems(ctx, dbMetaInfo, pagination, opts)
	if err != nil {
		c.logger.Error(ctx, "error listing items", "err", err)

		err = status.Error(codes.Internal, err.Error())
		return nil, err
	}

	items := make([]*grpc_ltngdb.Item, len(loadedItems.Items))
	for idx, item := range loadedItems.Items {
		items[idx] = &grpc_ltngdb.Item{
			Key:   item.Key,
			Value: item.Value,
		}
	}

	return &grpc_ltngdb.ListResponse{
		Pagination: &grpc_pagination.Pagination{
			PageId:           loadedItems.Pagination.PageID,
			PageSize:         loadedItems.Pagination.PageSize,
			PaginationCursor: loadedItems.Pagination.PaginationCursor,
		},
		Items: items,
	}, nil
}
