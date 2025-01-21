package ltngdb_controller_v2

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) Delete(
	ctx context.Context,
	req *grpc_ltngdb.DeleteRequest,
) (*grpc_ltngdb.DeleteResponse, error) {
	logger := c.logger.FromCtx(ctx)

	dbMetaInfo := &ltngenginemodels.ManagerStoreMetaInfo{
		Name: req.GetDatabaseMetaInfo().GetDatabaseName(),
		Path: req.GetDatabaseMetaInfo().GetDatabasePath(),
	}
	item := &ltngenginemodels.Item{
		Key: req.GetItem().GetKey(),
	}
	opts := &ltngenginemodels.IndexOpts{
		HasIdx:       req.GetIndexOpts().GetHasIdx(),
		ParentKey:    req.GetIndexOpts().GetParentKey(),
		IndexingKeys: req.GetIndexOpts().GetIndexingKeys(),
		IndexProperties: ltngenginemodels.IndexProperties{
			IndexDeletionBehaviour: ltngenginemodels.IndexDeletionBehaviour(
				req.GetIndexOpts().GetIndexingProperties().GetIndexDeletionBehaviour(),
			),
		},
	}
	if _, err := c.engine.DeleteItem(ctx, dbMetaInfo, item, opts); err != nil {
		logger.Errorf(
			"error deleting item",
			go_logger.Field{
				"err": err.Error(),
			},
		)

		err = status.Error(codes.Internal, err.Error())
		return nil, err
	}

	return &grpc_ltngdb.DeleteResponse{}, nil
}
