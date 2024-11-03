package badgerdb_operator_controller_v4

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v4/management"
	badgerdb_operation_models_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v4/operation"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

func (c *BadgerDBOperatorServiceControllerV4) List(
	ctx context.Context,
	req *grpc_ops.ListRequest,
) (*grpc_ops.ListResponse, error) {
	logger := c.logger.FromCtx(ctx)

	var r badgerdb_management_models_v4.Pagination
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
	opts := &badgerdb_operation_models_v4.IndexOpts{
		HasIdx:       reqOpts.GetHasIdx(),
		ParentKey:    reqOpts.GetParentKey(),
		IndexingKeys: reqOpts.GetIndexingKeys(),
		IndexProperties: badgerdb_operation_models_v4.IndexProperties{
			ListSearchPattern: badgerdb_operation_models_v4.ListSearchPattern(
				reqOpts.GetIndexingProperties().GetListSearchPattern(),
			),
		},
	}

	var operationList badgerdb_operation_models_v4.Items
	operationList, err = c.operator.Operate(dbInfo).List(ctx, opts, &r)
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
	operationList badgerdb_operation_models_v4.Items,
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
