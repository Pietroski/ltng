package badgerdb_operator_controller

import (
	"context"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

func (c *BadgerDBOperatorServiceController) Delete(
	ctx context.Context,
	req *grpc_ops.DeleteRequest,
) (*grpc_ops.DeleteResponse, error) {
	logger := c.logger.FromCtx(ctx)

	dbInfo, err := c.manager.GetDBMemoryInfo(ctx, req.GetDatabaseMetaInfo().GetDatabaseName())
	if err != nil {
		err = status.Error(codes.Internal, err.Error())
		logger.Errorf(
			"error getting db info from memory or disk",
			go_logger.Field{
				"error":   err.Error(),
				"request": req,
			},
		)
		return &grpc_ops.DeleteResponse{}, err
	}

	err = c.operator.Operate(dbInfo).Delete(req.GetKey())
	if err != nil {
		err = status.Error(codes.Internal, err.Error())
		logger.Errorf(
			"error operating on giving database- delete",
			go_logger.Field{
				"error":   err.Error(),
				"db_info": dbInfo,
				"request": req,
			},
		)
		return &grpc_ops.DeleteResponse{}, err
	}

	return &grpc_ops.DeleteResponse{}, nil
}
