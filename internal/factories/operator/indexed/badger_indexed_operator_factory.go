package badgerdb_indexed_operator_factory

import (
	indexed_manager "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager/indexed"
	indexed_operations "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/transactions/operations/indexed"
	badgerdb_indexed_operator_controller "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/controllers/operator/indexed"
	grpc_indexed_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations/indexed/indexed_operations"
	"net"

	"google.golang.org/grpc"

	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
)

type (
	BadgerDBIndexedOperatorServiceFactory struct {
		listener        net.Listener
		server          *grpc.Server
		logger          go_logger.Logger
		binder          go_binder.Binder
		indexedManager  indexed_manager.IndexerManager
		indexedOperator indexed_operations.IndexedOperator
	}
)

func NewBadgerDBIndexedOperatorService(
	listener net.Listener,
	logger go_logger.Logger,
	binder go_binder.Binder,
	indexedManager indexed_manager.IndexerManager,
	indexedOperator indexed_operations.IndexedOperator,
) *BadgerDBIndexedOperatorServiceFactory {
	factory := &BadgerDBIndexedOperatorServiceFactory{
		listener:        listener,
		logger:          logger,
		binder:          binder,
		indexedManager:  indexedManager,
		indexedOperator: indexedOperator,
	}
	factory.handle()

	return factory
}

func (s *BadgerDBIndexedOperatorServiceFactory) handle() {
	var grpcOpts []grpc.ServerOption
	grpcServer := grpc.NewServer(grpcOpts...)

	grpc_indexed_ops.RegisterIndexedOperationServer(
		grpcServer,
		badgerdb_indexed_operator_controller.NewBadgerDBIndexerOperatorServiceController(
			s.logger,
			s.binder,
			s.indexedManager,
			s.indexedOperator,
		),
	)

	s.server = grpcServer
}

func (s *BadgerDBIndexedOperatorServiceFactory) Start() error {
	return s.server.Serve(s.listener)
}

func (s *BadgerDBIndexedOperatorServiceFactory) Stop() {
	s.server.Stop()
}
