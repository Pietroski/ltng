package badgerdb_operator_controller_v4

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	badgerdb_operation_models_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v4/operation"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

func (c *BadgerDBOperatorServiceControllerV4) Load(
	ctx context.Context,
	req *grpc_ops.LoadRequest,
) (*grpc_ops.LoadResponse, error) {
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
		return &grpc_ops.LoadResponse{}, err
	}

	reqItem := req.GetItem()
	item := &badgerdb_operation_models_v4.Item{
		Key: reqItem.GetKey(),
	}

	reqOpts := req.GetIndexOpts()
	opts := &badgerdb_operation_models_v4.IndexOpts{
		HasIdx:       reqOpts.GetHasIdx(),
		ParentKey:    reqOpts.GetParentKey(),
		IndexingKeys: reqOpts.GetIndexingKeys(),
		IndexProperties: badgerdb_operation_models_v4.IndexProperties{
			IndexSearchPattern: badgerdb_operation_models_v4.IndexSearchPattern(
				reqOpts.GetIndexingProperties().GetIndexSearchPattern(),
			),
		},
	}

	var keyValue []byte
	keyValue, err = c.operator.Operate(dbInfo).Load(ctx, item, opts)
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
		return &grpc_ops.LoadResponse{}, err
	}

	return &grpc_ops.LoadResponse{
		Value: keyValue,
	}, nil
}
