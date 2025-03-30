package ltngdb_controller_v1

import (
	"context"

	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"

	ltng_engine_v1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v1"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config"
	common_model "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

type (
	Controller struct {
		grpc_ltngdb.UnimplementedLightningDBServer
		cfg *ltng_node_config.Config

		logger go_logger.Logger
		binder go_binder.Binder

		engine *ltng_engine_v1.LTNGEngine
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
					Engine: common_model.LightningEngineV1EngineVersionType.String(),
				},
				Server: &ltng_node_config.Server{
					Network: "tcp",
					Port:    "50050",
				},
			},
		},
		logger: go_logger.NewGoLogger(
			ctx, nil, go_logger.NewDefaultOpts(),
		).FromCtx(ctx),
		binder: go_binder.NewStructBinder(
			serializer.NewRawBinarySerializer(),
			go_validator.NewStructValidator(),
		),
	}
	options.ApplyOptions(c, opts...)

	return c
}
