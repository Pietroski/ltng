package badgerdb_controller_v4

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	badgerdb_operation_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/operation"
	list_operator "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/list-operator"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) Create(
	ctx context.Context,
	req *grpc_ltngdb.CreateRequest,
) (*grpc_ltngdb.CreateResponse, error) {
	dbInfo, err := c.manager.GetDBMemoryInfo(ctx, req.GetDatabaseMetaInfo().GetDatabaseName())
	if err != nil {
		err = status.Error(codes.Internal, err.Error())
		c.logger.Error(ctx, "error getting db info from memory or disk", "error", err)
		return &grpc_ltngdb.CreateResponse{}, err
	}

	reqItem := req.GetItem()
	item := &badgerdb_operation_models_v4.Item{
		Key:   reqItem.GetKey(),
		Value: reqItem.GetValue(),
	}

	reqOpts := req.GetIndexOpts()
	opts := &badgerdb_operation_models_v4.IndexOpts{
		HasIdx:       reqOpts.GetHasIdx(),
		ParentKey:    reqOpts.GetParentKey(),
		IndexingKeys: reqOpts.GetIndexingKeys(),
	}

	reqRetrialOpts := req.GetRetrialOpts()
	retrialOpts := &list_operator.RetrialOpts{
		RetrialOnErr: reqRetrialOpts.GetRetrialOnError(),
		RetrialCount: int(reqRetrialOpts.GetRetrialCount()),
	}

	err = c.operator.Operate(dbInfo).Create(
		ctx, item, opts, retrialOpts,
	)
	if err != nil {
		err = status.Error(codes.Internal, err.Error())
		c.logger.Error(ctx, "failed to create item", "db_info", dbInfo, "error", err)
		return &grpc_ltngdb.CreateResponse{}, err
	}

	return &grpc_ltngdb.CreateResponse{}, nil
}
