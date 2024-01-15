package badgerdb_manager_controller_v3

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
)

func (c *BadgerDBManagerServiceControllerV3) RestartLightningNode(
	_ context.Context,
	_ *grpc_mngmt.RestartLightningNodeRequest,
) (*grpc_mngmt.RestartLightningNodeResponse, error) {
	err := status.Error(codes.Unimplemented, fmt.Sprintf("unimplemented method. Implement it."))
	return &grpc_mngmt.RestartLightningNodeResponse{}, err
}
