package ltngdb_controller_v2

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) Create(
	ctx context.Context,
	req *grpc_ltngdb.CreateRequest,
) (*grpc_ltngdb.CreateResponse, error) {
	dbMetaInfo := &ltngdata.ManagerStoreMetaInfo{
		Name: req.GetDatabaseMetaInfo().GetDatabaseName(),
		Path: req.GetDatabaseMetaInfo().GetDatabasePath(),
	}
	item := &ltngdata.Item{
		Key:   req.GetItem().GetKey(),
		Value: req.GetItem().GetValue(),
	}
	opts := &ltngdata.IndexOpts{
		HasIdx:       req.GetIndexOpts().GetHasIdx(),
		ParentKey:    req.GetIndexOpts().GetParentKey(),
		IndexingKeys: req.GetIndexOpts().GetIndexingKeys(),
	}
	if _, err := c.engine.CreateItem(ctx, dbMetaInfo, item, opts); err != nil {
		c.logger.Error(ctx, "error creating item", "error", err)

		err = status.Error(codes.Internal, err.Error())
		return nil, err
	}

	return &grpc_ltngdb.CreateResponse{}, nil
}
