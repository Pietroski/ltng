package ltngdb_controller_v1

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	ltng_engine_v1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v1"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) DeleteStore(
	ctx context.Context,
	req *grpc_ltngdb.DeleteStoreRequest,
) (*grpc_ltngdb.DeleteStoreResponse, error) {
	logger := c.logger.FromCtx(ctx)

	payload := &ltng_engine_v1.StoreInfo{
		Name: req.GetName(),
		Path: req.GetPath(),
	}
	if err := c.engine.DeleteStore(ctx, payload); err != nil {
		logger.Errorf(
			"error creating store",
			go_logger.Field{
				"err":  err.Error(),
				"name": payload.Name,
				"path": payload.Path,
			},
		)

		err = status.Error(codes.Internal, err.Error())
		return nil, err
	}

	return &grpc_ltngdb.DeleteStoreResponse{}, nil
}
