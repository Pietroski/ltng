package badgerdb_controller_v4

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

func (c *Controller) ShutdownLightningNode(
	_ context.Context,
	_ *grpc_ltngdb.ShutdownLightningNodeRequest,
) (*grpc_ltngdb.ShutdownLightningNodeResponse, error) {
	err := status.Error(codes.Unimplemented, fmt.Sprintf("unimplemented method. Implement it."))
	return &grpc_ltngdb.ShutdownLightningNodeResponse{}, err
}
