package badgerdb_manager_factory_v3

import (
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_tracer_middleware "gitlab.com/pietroski-software-company/tools/middlewares/go-middlewares/pkg/tools/middlewares/gRPC/tracer"

	badgerdb_manager_adaptor_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/manager"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/config"
	badgerdb_manager_controller_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/controllers/badger/v3/manager"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
)

type (
	BadgerDBManagerServiceFactoryV3Params struct {
		Listener net.Listener
		Config   *ltng_node_config.Config
		Logger   go_logger.Logger
		Binder   go_binder.Binder
		Manager  badgerdb_manager_adaptor_v3.Manager
	}

	BadgerDBManagerServiceFactoryV3 struct {
		listener net.Listener
		cfg      *ltng_node_config.Config
		server   *grpc.Server
		logger   go_logger.Logger
		binder   go_binder.Binder
		manager  badgerdb_manager_adaptor_v3.Manager

		controller *badgerdb_manager_controller_v3.BadgerDBManagerServiceControllerV3
	}
)

func NewBadgerDBManagerServiceFactoryV3(
	params *BadgerDBManagerServiceFactoryV3Params,
) (*BadgerDBManagerServiceFactoryV3, error) {
	factory := &BadgerDBManagerServiceFactoryV3{
		listener: params.Listener,
		cfg:      params.Config,
		logger:   params.Logger,
		binder:   params.Binder,
		manager:  params.Manager,
	}

	controllerParams := &badgerdb_manager_controller_v3.BadgerDBManagerServiceControllerV3Params{
		Config:  factory.cfg,
		Logger:  factory.logger,
		Binder:  factory.binder,
		Manager: factory.manager,
	}
	controller, err := badgerdb_manager_controller_v3.NewBadgerDBManagerServiceControllerV3(controllerParams)
	if err != nil {
		return nil, fmt.Errorf("error creating NewBadgerDBManagerServiceControllerV3: %v", err)
	}
	factory.controller = controller

	factory.handle()

	return factory, nil
}

func (s *BadgerDBManagerServiceFactoryV3) handle() {
	grpcOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			go_tracer_middleware.NewGRPCUnaryTracerServerMiddleware(),
		),
	}
	grpcServer := grpc.NewServer(grpcOpts...)

	grpc_mngmt.RegisterManagementServer(grpcServer, s.controller)

	// Register reflection service on gRPC server.
	reflection.Register(grpcServer)

	s.server = grpcServer
}

func (s *BadgerDBManagerServiceFactoryV3) Start() error {
	return s.server.Serve(s.listener)
}

func (s *BadgerDBManagerServiceFactoryV3) Stop() {
	s.manager.ShutdownStores()
	s.manager.Shutdown()
	s.server.Stop()
}
