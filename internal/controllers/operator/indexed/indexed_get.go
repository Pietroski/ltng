package badgerdb_indexed_operator_controller

import (
	"context"
	indexed_operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation/indexed"
	grpc_indexed_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations/indexed/indexed_operations"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

func (c *BadgerDBIndexedOperatorServiceController) IndexedGet(
	ctx context.Context,
	req *grpc_indexed_ops.GetIndexedRequest,
) (*grpc_ops.GetResponse, error) {
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
		return &grpc_ops.GetResponse{}, err
	}

	var keyValue []byte
	item := indexed_operation_models.GetItemFromRequest(req.GetItem())
	index := indexed_operation_models.GetIndexFromRequest(req.GetIndex())
	keyValue, err = c.indexedOperator.Operate(dbInfo).LoadIndexed(ctx, item, index)
	if err != nil {
		err = status.Error(codes.Internal, err.Error())
		logger.Errorf(
			"error operating on giving database - load",
			go_logger.Field{
				"error":   err.Error(),
				"db_info": dbInfo,
				"request": req,
			},
		)
		return &grpc_ops.GetResponse{}, err
	}

	return &grpc_ops.GetResponse{
		Value: keyValue,
	}, nil
}
