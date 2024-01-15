package badgerdb_operator_controller_v3

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	badgerdb_operation_models_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v3/operation"
	chainded_operator "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

func (c *BadgerDBOperatorServiceControllerV3) Delete(
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

	reqItem := req.GetItem()
	item := &badgerdb_operation_models_v3.Item{
		Key: reqItem.GetKey(),
	}

	reqOpts := req.GetIndexOpts()
	opts := &badgerdb_operation_models_v3.IndexOpts{
		HasIdx:       reqOpts.GetHasIdx(),
		ParentKey:    reqOpts.GetParentKey(),
		IndexingKeys: reqOpts.GetIndexingKeys(),
		IndexProperties: badgerdb_operation_models_v3.IndexProperties{
			IndexDeletionBehaviour: badgerdb_operation_models_v3.IndexDeletionBehaviour(
				reqOpts.GetIndexingProperties().GetIndexDeletionBehaviour(),
			),
		},
	}

	reqRetrialOpts := req.GetRetrialOpts()
	retrialOpts := &chainded_operator.RetrialOpts{
		RetrialOnErr: reqRetrialOpts.GetRetrialOnError(),
		RetrialCount: int(reqRetrialOpts.GetRetrialCount()),
	}

	err = c.operator.Operate(dbInfo).Delete(
		ctx, item, opts, retrialOpts,
	)
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
