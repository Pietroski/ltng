package ltngdb_controller_v1

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	ltng_engine_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v2"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) CreateStore(
	ctx context.Context,
	req *grpc_ltngdb.CreateStoreRequest,
) (*grpc_ltngdb.CreateStoreResponse, error) {
	logger := c.logger.FromCtx(ctx)

	payload := &ltng_engine_v2.StoreInfo{
		Name: req.GetName(),
		Path: req.GetPath(),
	}
	info, err := c.engine.CreateStore(ctx, payload)
	if err != nil {
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

	return &grpc_ltngdb.CreateStoreResponse{
		CreatedAt:    timestamppb.New(time.Unix(info.CreatedAt, 0)),
		LastOpenedAt: timestamppb.New(time.Unix(info.LastOpenedAt, 0)),
	}, nil
}
