package badgerdb_operator_controller

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

func (c *BadgerDBOperatorServiceController) Load(
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
	item := &operation_models.Item{
		Key: reqItem.GetKey(),
	}

	reqOpts := req.GetIndexOpts()
	opts := &operation_models.IndexOpts{
		HasIdx:       reqOpts.GetHasIdx(),
		ParentKey:    reqOpts.GetParentKey(),
		IndexingKeys: reqOpts.GetIndexingKeys(),
		IndexProperties: operation_models.IndexProperties{
			IndexSearchPattern: operation_models.IndexSearchPattern(
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
