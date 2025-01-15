package ltngdb_controller_v1

import (
	"context"
	ltng_engine_models "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltng-engine"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	ltng_engine_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v2"
	grpc_pagination "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/common/search"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) List(
	ctx context.Context,
	req *grpc_ltngdb.ListRequest,
) (*grpc_ltngdb.ListResponse, error) {
	logger := c.logger.FromCtx(ctx)

	dbMetaInfo := &ltng_engine_v2.ManagerStoreMetaInfo{
		Name: req.GetDatabaseMetaInfo().GetDatabaseName(),
		Path: req.GetDatabaseMetaInfo().GetDatabasePath(),
	}
	pagination := &ltng_engine_models.Pagination{
		PageID:           req.GetPagination().GetPageId(),
		PageSize:         req.GetPagination().GetPageSize(),
		PaginationCursor: req.GetPagination().GetPaginationCursor(),
	}
	opts := &ltng_engine_v2.IndexOpts{
		HasIdx:       req.GetIndexOpts().GetHasIdx(),
		ParentKey:    req.GetIndexOpts().GetParentKey(),
		IndexingKeys: req.GetIndexOpts().GetIndexingKeys(),
		IndexProperties: ltng_engine_v2.IndexProperties{
			ListSearchPattern: ltng_engine_v2.ListSearchPattern(
				req.GetIndexOpts().GetIndexingProperties().GetListSearchPattern(),
			),
		},
	}
	loadedItems, err := c.engine.ListItems(ctx, dbMetaInfo, pagination, opts)
	if err != nil {
		logger.Errorf(
			"error listing items",
			go_logger.Field{
				"err": err.Error(),
			},
		)

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
