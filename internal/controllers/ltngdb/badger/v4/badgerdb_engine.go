package badgerdb_controller_v4

import (
	"context"

	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"

	"gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	common_model "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

type (
	Controller struct {
		grpc_ltngdb.UnimplementedLightningDBServer

		cfg      *ltng_node_config.Config
		logger   go_logger.Logger
		binder   go_binder.Binder
		manager  v4.Manager
		operator v4.Operator
	}
)

func New(
	ctx context.Context,
	opts ...options.Option,
) (*Controller, error) {
	c := defaultControllerV4(ctx, opts...)
	return c, nil
}

func defaultControllerV4(
	ctx context.Context,
	opts ...options.Option,
) *Controller {
	c := &Controller{
		cfg: &ltng_node_config.Config{
			Node: &ltng_node_config.Node{
				Engine: &ltng_node_config.Engine{
					Engine: common_model.BadgerDBV4EngineVersionType.String(),
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
		manager:  nil,
		operator: nil,
	}
	options.ApplyOptions(c, opts...)

	return c
}
