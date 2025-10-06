package badgerdb_controller_v4

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	badgerdb_operation_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/operation"
	list_operator "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/list-operator"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) Delete(
	ctx context.Context,
	req *grpc_ltngdb.DeleteRequest,
) (*grpc_ltngdb.DeleteResponse, error) {
	dbInfo, err := c.manager.GetDBMemoryInfo(ctx, req.GetDatabaseMetaInfo().GetDatabaseName())
	if err != nil {
		err = status.Error(codes.Internal, err.Error())
		c.logger.Error(ctx, "error getting db info from memory or disk", "error", err)
		return &grpc_ltngdb.DeleteResponse{}, err
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
			IndexDeletionBehaviour: badgerdb_operation_models_v4.IndexDeletionBehaviour(
				reqOpts.GetIndexingProperties().GetIndexDeletionBehaviour(),
			),
		},
	}

	reqRetrialOpts := req.GetRetrialOpts()
	retrialOpts := &list_operator.RetrialOpts{
		RetrialOnErr: reqRetrialOpts.GetRetrialOnError(),
		RetrialCount: int(reqRetrialOpts.GetRetrialCount()),
	}

	err = c.operator.Operate(dbInfo).Delete(
		ctx, item, opts, retrialOpts,
	)
	if err != nil {
		err = status.Error(codes.Internal, err.Error())
		c.logger.Error(ctx, "error deleting item", "db_info", dbInfo, "error", err)
		return &grpc_ltngdb.DeleteResponse{}, err
	}

	return &grpc_ltngdb.DeleteResponse{}, nil
}
