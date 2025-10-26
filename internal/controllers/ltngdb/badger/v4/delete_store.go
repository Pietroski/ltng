package badgerdb_controller_v4

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) DeleteStore(
	ctx context.Context,
	req *grpc_ltngdb.DeleteStoreRequest,
) (*grpc_ltngdb.DeleteStoreResponse, error) {
	if err := c.manager.DeleteStore(ctx, req.GetName()); err != nil {
		c.logger.Error(ctx, "error deleting store", "name", req.GetName(), "error", err)

		err = status.Error(codes.Internal, err.Error())
		return &grpc_ltngdb.DeleteStoreResponse{}, err
	}

	return &grpc_ltngdb.DeleteStoreResponse{}, nil
}
