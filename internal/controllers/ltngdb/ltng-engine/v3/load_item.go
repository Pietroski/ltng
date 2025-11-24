package ltngdbcontrollerv3

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ltngdbenginemodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) Load(
	ctx context.Context,
	req *grpc_ltngdb.LoadRequest,
) (*grpc_ltngdb.LoadResponse, error) {
	dbMetaInfo := &ltngdbenginemodelsv3.ManagerStoreMetaInfo{
		Name: req.GetDatabaseMetaInfo().GetDatabaseName(),
		Path: req.GetDatabaseMetaInfo().GetDatabasePath(),
	}
	item := &ltngdbenginemodelsv3.Item{
		Key: req.GetItem().GetKey(),
	}
	opts := &ltngdbenginemodelsv3.IndexOpts{
		HasIdx:       req.GetIndexOpts().GetHasIdx(),
		ParentKey:    req.GetIndexOpts().GetParentKey(),
		IndexingKeys: req.GetIndexOpts().GetIndexingKeys(),
		IndexProperties: ltngdbenginemodelsv3.IndexProperties{
			IndexSearchPattern: ltngdbenginemodelsv3.IndexSearchPattern(
				req.GetIndexOpts().GetIndexingProperties().GetIndexSearchPattern(),
			),
		},
	}
	loadedItem, err := c.engine.LoadItem(ctx, dbMetaInfo, item, opts)
	if err != nil {
		c.logger.Error(ctx, "error loading item", "err", err)

		err = status.Error(codes.Internal, err.Error())
		return nil, err
	}

	return &grpc_ltngdb.LoadResponse{
		Value: loadedItem.Value,
	}, nil
}
