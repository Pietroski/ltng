package badgerdb_indexed_operator_controller

import (
	"context"
	indexed_operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation/indexed"
	grpc_indexed_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations/indexed/indexed_operations"

	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

func (c *BadgerDBIndexedOperatorServiceController) IndexedList(
	ctx context.Context,
	req *grpc_indexed_ops.ListIndexedRequest,
) (*grpc_ops.ListResponse, error) {
	logger := c.logger.FromCtx(ctx)

	var r management_models.PaginationRequest
	if err := c.binder.ShouldBind(req.Pagination, &r); err != nil {
		err = status.Error(codes.InvalidArgument, err.Error())
		logger.Errorf(
			"error binding and/or validating payload data",
			go_logger.Field{
				"error":   err.Error(),
				"request": req,
			},
		)
		return &grpc_ops.ListResponse{}, err
	}

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
		return &grpc_ops.ListResponse{}, err
	}

	var operationList operation_models.OpList

	index := indexed_operation_models.GetIndexFromRequest(req.GetIndex())
	operationList, err = c.indexedOperator.Operate(dbInfo).ListIndexed(ctx, index, &r)
	if err != nil {
		err = status.Error(codes.Internal, err.Error())
		logger.Errorf(
			"error operating on giving database - list",
			go_logger.Field{
				"error":   err.Error(),
				"db_info": dbInfo,
				"request": req,
			},
		)
		return &grpc_ops.ListResponse{}, err
	}

	itt := operationListRemapper(operationList)
	return &grpc_ops.ListResponse{
		Items: itt,
	}, nil
}

func operationListRemapper(
	operationList operation_models.OpList,
) []*grpc_ops.Item {
	remappedOpsList := make([]*grpc_ops.Item, len(operationList))
	for idx, item := range operationList {
		if item == nil {
			remappedOpsList[idx] = &grpc_ops.Item{}

			continue
		}

		opsItem := &grpc_ops.Item{
			Key:   item.Key,
			Value: item.Value,
		}

		remappedOpsList[idx] = opsItem
	}

	return remappedOpsList
}
