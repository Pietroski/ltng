package badgerdb_controller_v4

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) GetStore(
	ctx context.Context,
	req *grpc_ltngdb.GetStoreRequest,
) (*grpc_ltngdb.GetStoreResponse, error) {
	dbInfo, err := c.manager.GetDBInfo(ctx, req.GetName())
	if err != nil {
		c.logger.Error(ctx, "error getting db info", "name", req.GetName(), "error", err)

		err = status.Error(codes.Internal, err.Error())
		return &grpc_ltngdb.GetStoreResponse{}, err
	}

	respDbInfo := &grpc_ltngdb.DBInfo{
		Name:         dbInfo.Name,
		Path:         dbInfo.Path,
		CreatedAt:    timestamppb.New(dbInfo.CreatedAt),
		LastOpenedAt: timestamppb.New(dbInfo.LastOpenedAt),
	}
	resp := &grpc_ltngdb.GetStoreResponse{DbInfo: respDbInfo}

	return resp, nil
}
