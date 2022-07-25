package badgerdb_manager_factory

import (
	"net"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager"
	badgerdb_manager_controller "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/controllers/manager"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"google.golang.org/grpc"
)

type (
	BadgerDBManagerServiceFactory struct {
		listener net.Listener
		server   *grpc.Server

		logger go_logger.Logger
		//serializer go_serializer.Serializer
		//validator  go_validator.Validator
		binder go_binder.Binder

		manager manager.Manager
	}
)

func NewBadgerDBManagerService(
	listener net.Listener,

	logger go_logger.Logger,
	//serializer go_serializer.Serializer,
	//validator go_validator.Validator,
	binder go_binder.Binder,

	manager manager.Manager,
) *BadgerDBManagerServiceFactory {
	factory := &BadgerDBManagerServiceFactory{
		listener: listener,

		logger: logger,
		//serializer: serializer,
		//validator:  validator,
		binder: binder,

		manager: manager,
	}
	factory.handle()

	return factory
}

func (s *BadgerDBManagerServiceFactory) handle() {
	var grpcOpts []grpc.ServerOption
	grpcServer := grpc.NewServer(grpcOpts...)

	grpc_mngmt.RegisterManagementServer(
		grpcServer,
		badgerdb_manager_controller.NewBadgerDBServiceController(
			s.logger,
			//s.serializer,
			//s.validator,
			s.binder,

			s.manager,
		),
	)

	s.server = grpcServer
}

func (s *BadgerDBManagerServiceFactory) Start() error {
	return s.server.Serve(s.listener)
}

func (s *BadgerDBManagerServiceFactory) Stop() {
	s.manager.ShutdownStores()
	s.manager.Shutdown()
	s.server.Stop()
}
