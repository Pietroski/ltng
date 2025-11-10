package ltngdb_controller_v2

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) CreateStore(
	ctx context.Context,
	req *grpc_ltngdb.CreateStoreRequest,
) (*grpc_ltngdb.CreateStoreResponse, error) {
	payload := &ltngdata.StoreInfo{
		Name: req.GetName(),
		Path: req.GetPath(),
	}
	info, err := c.engine.CreateStore(ctx, payload)
	if err != nil {
		c.logger.Error(ctx, "error creating store",
			"name", payload.Name, "path", payload.Path, "error", err)

		err = status.Error(codes.Internal, err.Error())
		return nil, err
	}

	return &grpc_ltngdb.CreateStoreResponse{
		CreatedAt:    timestamppb.New(time.Unix(info.CreatedAt, 0)),
		LastOpenedAt: timestamppb.New(time.Unix(info.LastOpenedAt, 0)),
	}, nil
}
