package badgerdb_operator_controller

import (
	"context"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/schemas/generated/go/transactions/operations"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (c *BadgerDBOperatorServiceController) Set(
	ctx context.Context,
	req *grpc_ops.SetRequest,
) (*grpc_ops.SetResponse, error) {
	logger := c.logger.FromCtx(ctx)

	dbInfo, err := c.manager.GetDBMemoryInfo(ctx, req.DatabaseMetaInfo.DatabaseName)
	if err != nil {
		err = status.Error(codes.Internal, err.Error())
		logger.Errorf(
			"error getting db info from memory or disk",
			go_logger.Field{
				"error":   err.Error(),
				"request": req,
			},
		)
		return &grpc_ops.SetResponse{}, err
	}

	err = c.operator.Operate(dbInfo).Update(req.Item.Key, req.Item.Value)
	if err != nil {
		err = status.Error(codes.Internal, err.Error())
		logger.Errorf(
			"error operating on giving database - update",
			go_logger.Field{
				"error":   err.Error(),
				"db_info": dbInfo,
				"request": req,
			},
		)
		return &grpc_ops.SetResponse{}, err
	}

	return &grpc_ops.SetResponse{}, nil
}
