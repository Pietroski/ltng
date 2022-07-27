package badgerdb_indexed_manager_factory

import (
	indexed_manager "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager/indexed"
	badgerdb_indexed_manager_controller "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/controllers/manager/indexed"
	grpc_indexed_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management/indexed/indexed_management"
	"net"

	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"google.golang.org/grpc"
)

type (
	BadgerDBIndexedManagerServiceFactory struct {
		listener       net.Listener
		server         *grpc.Server
		logger         go_logger.Logger
		binder         go_binder.Binder
		indexedManager indexed_manager.IndexerManager
	}
)

func NewBadgerDBIndexedManagerService(
	listener net.Listener,
	logger go_logger.Logger,
	binder go_binder.Binder,
	indexedManager indexed_manager.IndexerManager,
) *BadgerDBIndexedManagerServiceFactory {
	factory := &BadgerDBIndexedManagerServiceFactory{
		listener:       listener,
		logger:         logger,
		binder:         binder,
		indexedManager: indexedManager,
	}
	factory.handle()

	return factory
}

func (s *BadgerDBIndexedManagerServiceFactory) handle() {
	var grpcOpts []grpc.ServerOption
	grpcServer := grpc.NewServer(grpcOpts...)

	grpc_indexed_mngmt.RegisterIndexedManagementServer(
		grpcServer,
		badgerdb_indexed_manager_controller.NewBadgerDBIndexerManagerServiceController(
			s.logger,
			s.binder,
			s.indexedManager,
		),
	)

	s.server = grpcServer
}

func (s *BadgerDBIndexedManagerServiceFactory) Start() error {
	return s.server.Serve(s.listener)
}

func (s *BadgerDBIndexedManagerServiceFactory) Stop() {
	s.server.Stop()
}
