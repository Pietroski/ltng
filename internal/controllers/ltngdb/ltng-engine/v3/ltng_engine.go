package ltngdbcontrollerv3

import (
	"context"

	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	ltngdbenginev3 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltngdbengine/v3"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	common_model "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

type (
	Controller struct {
		grpc_ltngdb.UnimplementedLightningDBServer
		cfg *ltng_node_config.Config

		logger slogx.SLogger
		engine *ltngdbenginev3.LTNGEngine
	}
)

func New(
	ctx context.Context,
	opts ...options.Option,
) (*Controller, error) {
	c := defaultController(ctx, opts...)
	options.ApplyOptions(c, opts...)

	return c, nil
}

func defaultController(
	ctx context.Context,
	opts ...options.Option,
) *Controller {
	c := &Controller{
		cfg: &ltng_node_config.Config{
			Node: &ltng_node_config.Node{
				Engine: &ltng_node_config.Engine{
					Engine: common_model.LightningEngineV3EngineVersionType.String(),
				},
				Server: &ltng_node_config.Server{
					Network: "tcp",
					Port:    "50050",
				},
			},
		},
		logger: slogx.New(),
	}
	options.ApplyOptions(c, opts...)

	return c
}
