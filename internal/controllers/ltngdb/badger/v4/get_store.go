package badgerdb_controller_v4

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) GetStore(
	ctx context.Context,
	req *grpc_ltngdb.GetStoreRequest,
) (*grpc_ltngdb.GetStoreResponse, error) {
	var r badgerdb_management_models_v4.GetStoreRequest
	if err := c.binder.ShouldBind(req, &r); err != nil {
		c.logger.Error(ctx, "error binding data", "error", err)

		err = status.Error(codes.InvalidArgument, err.Error())
		return &grpc_ltngdb.GetStoreResponse{}, err
	}

	dbInfo, err := c.manager.GetDBInfo(ctx, r.Name)
	if err != nil {
		c.logger.Error(ctx, "error getting db info", "error", err)

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
