package badgerdb_manager_controller_v4

import (
	"context"

	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"

	badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/manager"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/config"
	common_model "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/common"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
)

type (
	BadgerDBManagerServiceControllerV4Params struct {
		Config *ltng_node_config.Config

		Logger  go_logger.Logger
		Binder  go_binder.Binder
		Manager badgerdb_manager_adaptor_v4.Manager
	}

	BadgerDBManagerServiceControllerV4 struct {
		grpc_mngmt.UnimplementedManagementServer
		cfg     *ltng_node_config.Config
		logger  go_logger.Logger
		binder  go_binder.Binder
		manager badgerdb_manager_adaptor_v4.Manager
	}
)

func NewBadgerDBManagerServiceControllerV4(
	ctx context.Context,
	opts ...options.Option,
) (*BadgerDBManagerServiceControllerV4, error) {
	c := defaultBadgerDBManagerServiceControllerV4(ctx, opts...)
	options.ApplyOptions(c, opts...)

	return c, nil
}

func defaultBadgerDBManagerServiceControllerV4(
	ctx context.Context,
	opts ...options.Option,
) *BadgerDBManagerServiceControllerV4 {
	c := &BadgerDBManagerServiceControllerV4{
		cfg: &ltng_node_config.Config{
			LTNGNode: &ltng_node_config.LTNGNode{
				LTNGEngine: &ltng_node_config.LTNGEngine{
					Engine: common_model.BadgerDBV4EngineVersionType.String(),
				},
				LTNGManager: &ltng_node_config.LTNGManager{
					Network: "tcp",
					Port:    "50051",
				},
				LTNGOperator: &ltng_node_config.LTNGOperator{
					Network: "tcp",
					Port:    "50052",
				},
			},
		},
		logger: go_logger.NewGoLogger(
			ctx, nil, go_logger.NewDefaultOpts(),
		).FromCtx(ctx),
		binder: go_binder.NewStructBinder(
			serializer.NewJsonSerializer(),
			go_validator.NewStructValidator(),
		),
	}
	options.ApplyOptions(c, opts...)

	return c
}
