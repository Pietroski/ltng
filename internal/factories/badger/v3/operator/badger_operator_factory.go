package badgerdb_operator_factory_v3

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	badgerdb_manager_adaptor_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/manager"
	badgerdb_operations_adaptor_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/transactions/operations"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/config"
	badgerdb_operator_controller_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/controllers/badger/v3/operator"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

type (
	BadgerDBServiceOperatorFactoryV3Params struct {
		Listener net.Listener
		Config   *ltng_node_config.Config
		Logger   go_logger.Logger
		Binder   go_binder.Binder

		Manager  badgerdb_manager_adaptor_v3.Manager
		Operator badgerdb_operations_adaptor_v3.Operator
	}

	BadgerDBServiceOperatorFactoryV3 struct {
		listener net.Listener
		server   *grpc.Server
		cfg      *ltng_node_config.Config
		logger   go_logger.Logger
		binder   go_binder.Binder

		manager  badgerdb_manager_adaptor_v3.Manager
		operator badgerdb_operations_adaptor_v3.Operator

		controller *badgerdb_operator_controller_v3.BadgerDBOperatorServiceControllerV3
	}
)

func NewBadgerDBOperatorServiceFactoryV3(
	params *BadgerDBServiceOperatorFactoryV3Params,
) (*BadgerDBServiceOperatorFactoryV3, error) {
	factory := &BadgerDBServiceOperatorFactoryV3{
		listener: params.Listener,

		cfg:      params.Config,
		logger:   params.Logger,
		binder:   params.Binder,
		manager:  params.Manager,
		operator: params.Operator,
	}

	controllerParams := &badgerdb_operator_controller_v3.BadgerDBOperatorServiceControllerV3Params{
		Config:   factory.cfg,
		Logger:   factory.logger,
		Binder:   factory.binder,
		Manager:  factory.manager,
		Operator: factory.operator,
	}
	controller, err := badgerdb_operator_controller_v3.NewBadgerDBOperatorServiceControllerV3(controllerParams)
	if err != nil {
		return nil, fmt.Errorf("error creating NewBadgerDBOperatorServiceControllerV3: %v", err)
	}
	factory.controller = controller

	factory.handle()

	return factory, nil
}

func (s *BadgerDBServiceOperatorFactoryV3) handle() {
	var grpcOpts []grpc.ServerOption
	grpcServer := grpc.NewServer(grpcOpts...)

	grpc_ops.RegisterOperationServer(grpcServer, s.controller)

	s.server = grpcServer
}

func (s *BadgerDBServiceOperatorFactoryV3) Start() error {
	return s.server.Serve(s.listener)
}

func (s *BadgerDBServiceOperatorFactoryV3) Stop() {
	s.server.Stop()
}
