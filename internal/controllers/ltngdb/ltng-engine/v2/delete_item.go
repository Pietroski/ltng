package ltngdb_controller_v2

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) Delete(
	ctx context.Context,
	req *grpc_ltngdb.DeleteRequest,
) (*grpc_ltngdb.DeleteResponse, error) {
	dbMetaInfo := &ltngdata.ManagerStoreMetaInfo{
		Name: req.GetDatabaseMetaInfo().GetDatabaseName(),
		Path: req.GetDatabaseMetaInfo().GetDatabasePath(),
	}
	item := &ltngdata.Item{
		Key: req.GetItem().GetKey(),
	}
	opts := &ltngdata.IndexOpts{
		HasIdx:       req.GetIndexOpts().GetHasIdx(),
		ParentKey:    req.GetIndexOpts().GetParentKey(),
		IndexingKeys: req.GetIndexOpts().GetIndexingKeys(),
		IndexProperties: ltngdata.IndexProperties{
			IndexDeletionBehaviour: ltngdata.IndexDeletionBehaviour(
				req.GetIndexOpts().GetIndexingProperties().GetIndexDeletionBehaviour(),
			),
		},
	}
	if _, err := c.engine.DeleteItem(ctx, dbMetaInfo, item, opts); err != nil {
		c.logger.Error(ctx, "error deleting item", "error", err)

		err = status.Error(codes.Internal, err.Error())
		return nil, err
	}

	return &grpc_ltngdb.DeleteResponse{}, nil
}
