package ltngdb_controller_v1

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	ltng_engine_v1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v1"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) Upsert(
	ctx context.Context,
	req *grpc_ltngdb.UpsertRequest,
) (*grpc_ltngdb.UpsertResponse, error) {
	logger := c.logger.FromCtx(ctx)

	dbMetaInfo := &ltng_engine_v1.ManagerStoreMetaInfo{
		Name: req.GetDatabaseMetaInfo().GetDatabaseName(),
		Path: req.GetDatabaseMetaInfo().GetDatabasePath(),
	}
	item := &ltng_engine_v1.Item{
		Key:   req.GetItem().GetKey(),
		Value: req.GetItem().GetValue(),
	}
	opts := &ltng_engine_v1.IndexOpts{
		HasIdx:       req.GetIndexOpts().GetHasIdx(),
		ParentKey:    req.GetIndexOpts().GetParentKey(),
		IndexingKeys: req.GetIndexOpts().GetIndexingKeys(),
	}
	if _, err := c.engine.UpsertItem(ctx, dbMetaInfo, item, opts); err != nil {
		logger.Errorf(
			"error creating item",
			go_logger.Field{
				"err": err.Error(),
			},
		)

		err = status.Error(codes.Internal, err.Error())
		return nil, err
	}

	return &grpc_ltngdb.UpsertResponse{}, nil
}
