package badgerdb_controller_v4

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	common_model "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
	grpc_query_config "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/common/queries/config"
)

func (c *Controller) CheckLightingNodeEngine(
	ctx context.Context,
	req *grpc_query_config.CheckEngineRequest,
) (*grpc_query_config.CheckEngineResponse, error) {
	logger := c.logger.FromCtx(ctx)

	if req.GetEngine() != "" && common_model.ToEngineVersionType(c.cfg.Node.Engine.Engine) !=
		common_model.ToEngineVersionType(req.GetEngine()) {
		err := status.Error(codes.FailedPrecondition,
			fmt.Sprintf("client and server version are incompatible"))

		logger.Errorf("client and server version are incompatible",
			go_logger.Field{
				"server_engine": common_model.ToEngineVersionType(c.cfg.Node.Engine.Engine),
				"client_engine": common_model.ToEngineVersionType(req.GetEngine()),
				"error":         err.Error(),
			},
		)

		return nil, err
	}

	return &grpc_query_config.CheckEngineResponse{}, nil
}
