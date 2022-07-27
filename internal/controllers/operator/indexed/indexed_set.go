package badgerdb_indexed_operator_controller

import (
	"context"
	indexed_operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation/indexed"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
	grpc_indexed_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations/indexed/indexed_operations"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (c *BadgerDBIndexedOperatorServiceController) IndexedSet(
	ctx context.Context,
	req *grpc_indexed_ops.SetIndexedRequest,
) (*grpc_ops.SetResponse, error) {
	logger := c.logger.FromCtx(ctx)

	dbInfo, err := c.indexedManager.GetDBMemoryInfo(ctx, req.DatabaseMetaInfo.DatabaseName)
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

	item := indexed_operation_models.GetItemFromRequest(req.GetItem())
	index := indexed_operation_models.GetIndexFromRequest(req.GetIndex())
	err = c.indexedOperator.Operate(dbInfo).UpdateIndexed(ctx, item, index)
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
