package ltngdb_controller_v1

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	ltng_engine_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v2"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) Load(
	ctx context.Context,
	req *grpc_ltngdb.LoadRequest,
) (*grpc_ltngdb.LoadResponse, error) {
	logger := c.logger.FromCtx(ctx)

	dbMetaInfo := &ltng_engine_v2.ManagerStoreMetaInfo{
		Name: req.GetDatabaseMetaInfo().GetDatabaseName(),
		Path: req.GetDatabaseMetaInfo().GetDatabasePath(),
	}
	item := &ltng_engine_v2.Item{
		Key: req.GetItem().GetKey(),
	}
	opts := &ltng_engine_v2.IndexOpts{
		HasIdx:       req.GetIndexOpts().GetHasIdx(),
		ParentKey:    req.GetIndexOpts().GetParentKey(),
		IndexingKeys: req.GetIndexOpts().GetIndexingKeys(),
		IndexProperties: ltng_engine_v2.IndexProperties{
			IndexSearchPattern: ltng_engine_v2.IndexSearchPattern(
				req.GetIndexOpts().GetIndexingProperties().GetIndexSearchPattern(),
			),
		},
	}
	loadedItem, err := c.engine.LoadItem(ctx, dbMetaInfo, item, opts)
	if err != nil {
		logger.Errorf(
			"error loading item",
			go_logger.Field{
				"err": err.Error(),
			},
		)

		err = status.Error(codes.Internal, err.Error())
		return nil, err
	}

	return &grpc_ltngdb.LoadResponse{
		Value: loadedItem.Value,
	}, nil
}
