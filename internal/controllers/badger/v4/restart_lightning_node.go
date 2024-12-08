package badgerdb_controller_v4

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) RestartLightningNode(
	_ context.Context,
	_ *grpc_ltngdb.RestartLightningNodeRequest,
) (*grpc_ltngdb.RestartLightningNodeResponse, error) {
	err := status.Error(codes.Unimplemented, fmt.Sprintf("unimplemented method. Implement it."))
	return &grpc_ltngdb.RestartLightningNodeResponse{}, err
}
