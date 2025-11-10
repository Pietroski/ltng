package ltngdb_controller_v2

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) DeleteStore(
	ctx context.Context,
	req *grpc_ltngdb.DeleteStoreRequest,
) (*grpc_ltngdb.DeleteStoreResponse, error) {
	payload := &ltngdata.StoreInfo{
		Name: req.GetName(),
		Path: req.GetPath(),
	}
	if err := c.engine.DeleteStore(ctx, payload); err != nil {
		c.logger.Error(ctx, "error deleting store", "name",
			payload.Name, "path", payload.Path, "err", err)

		err = status.Error(codes.Internal, err.Error())
		return nil, err
	}

	return &grpc_ltngdb.DeleteStoreResponse{}, nil
}
