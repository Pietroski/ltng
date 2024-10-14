package badgerdb_operator_controller_v4

import (
	"context"
	common_model "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/common"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"

	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/manager"
	badgerdb_operations_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/transactions/operations"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/config"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

type (
	BadgerDBOperatorServiceControllerV4 struct {
		grpc_ops.UnimplementedOperationServer
		cfg      *ltng_node_config.Config
		logger   go_logger.Logger
		binder   go_binder.Binder
		manager  badgerdb_manager_adaptor_v4.Manager
		operator badgerdb_operations_adaptor_v4.Operator
	}
)

func NewBadgerDBOperatorServiceControllerV4(
	ctx context.Context,
	opts ...options.Option,
) (*BadgerDBOperatorServiceControllerV4, error) {
	c := defaultBadgerDBOperatorServiceControllerV4(ctx, opts...)
	options.ApplyOptions(c, opts...)

	return c, nil
}

func defaultBadgerDBOperatorServiceControllerV4(
	ctx context.Context,
	opts ...options.Option,
) *BadgerDBOperatorServiceControllerV4 {
	c := &BadgerDBOperatorServiceControllerV4{
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
			go_serializer.NewJsonSerializer(),
			go_validator.NewStructValidator(),
		),
		manager:  nil,
		operator: nil,
	}
	options.ApplyOptions(c, opts...)

	return c
}
