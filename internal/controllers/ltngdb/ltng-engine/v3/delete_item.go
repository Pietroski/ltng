package ltngdbcontrollerv3

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ltngdbenginemodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) Delete(
	ctx context.Context,
	req *grpc_ltngdb.DeleteRequest,
) (*grpc_ltngdb.DeleteResponse, error) {
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
			IndexDeletionBehaviour: ltngdbenginemodelsv3.IndexDeletionBehaviour(
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
