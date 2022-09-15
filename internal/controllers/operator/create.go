package badgerdb_operator_controller

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation"
	chainded_operator "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

func (c *BadgerDBOperatorServiceController) Create(
	ctx context.Context,
	req *grpc_ops.CreateRequest,
) (*grpc_ops.CreateResponse, error) {
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
		return &grpc_ops.CreateResponse{}, err
	}

	reqItem := req.GetItem()
	item := &operation_models.Item{
		Key:   reqItem.GetKey(),
		Value: reqItem.GetValue(),
	}

	reqOpts := req.GetIndexOpts()
	opts := &operation_models.IndexOpts{
		HasIdx:       reqOpts.GetHasIdx(),
		ParentKey:    reqOpts.GetParentKey(),
		IndexingKeys: reqOpts.GetIndexingKeys(),
	}

	reqRetrialOpts := req.GetRetrialOpts()
	retrialOpts := &chainded_operator.RetrialOpts{
		RetrialOnErr: reqRetrialOpts.GetRetrialOnError(),
		RetrialCount: int(reqRetrialOpts.GetRetrialCount()),
	}

	err = c.operator.Operate(dbInfo).Create(
		ctx, item, opts, retrialOpts,
	)
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
		return &grpc_ops.CreateResponse{}, err
	}

	return &grpc_ops.CreateResponse{}, nil
}
