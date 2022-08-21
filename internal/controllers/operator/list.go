package badgerdb_operator_controller

import (
	"context"

	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

func (c *BadgerDBOperatorServiceController) List(
	ctx context.Context,
	req *grpc_ops.ListRequest,
) (*grpc_ops.ListResponse, error) {
	logger := c.logger.FromCtx(ctx)

	var r management_models.Pagination
	if err := c.binder.ShouldBind(req.GetPagination(), &r); err != nil {
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
		return &grpc_ops.ListResponse{}, err
	}

	reqOpts := req.GetIndexOpts()
	opts := &operation_models.IndexOpts{
		HasIdx:       reqOpts.GetHasIdx(),
		ParentKey:    reqOpts.GetParentKey(),
		IndexingKeys: reqOpts.GetIndexingKeys(),
		IndexProperties: operation_models.IndexProperties{
			ListSearchPattern: operation_models.ListSearchPattern(
				reqOpts.GetIndexingProperties().GetListSearchPattern(),
			),
		},
	}

	var operationList operation_models.Items
	operationList, err = c.operator.Operate(dbInfo).List(opts, &r)
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
	operationList operation_models.Items,
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
