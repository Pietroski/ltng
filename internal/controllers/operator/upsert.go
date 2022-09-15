package badgerdb_operator_controller

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

func (c *BadgerDBOperatorServiceController) Upsert(
	ctx context.Context,
	req *grpc_ops.UpsertRequest,
) (*grpc_ops.UpsertResponse, error) {
	err := status.Error(codes.Unimplemented, fmt.Sprintf("unimplemented method. Implement it."))
	return &grpc_ops.UpsertResponse{}, err
}
