package badgerdb_indexed_operator_controller

import (
	indexed_manager "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager/indexed"
	indexed_operations "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/transactions/operations/indexed"
	grpc_indexed_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations/indexed/indexed_operations"

	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
)

type (
	BadgerDBIndexedOperatorServiceController struct {
		grpc_indexed_ops.UnimplementedIndexedOperationServer
		logger          go_logger.Logger
		binder          go_binder.Binder
		indexedManager  indexed_manager.IndexerManager
		indexedOperator indexed_operations.IndexedOperator
	}
)

func NewBadgerDBIndexerOperatorServiceController(
	logger go_logger.Logger,
	binder go_binder.Binder,
	indexedManager indexed_manager.IndexerManager,
	indexedOperator indexed_operations.IndexedOperator,
) *BadgerDBIndexedOperatorServiceController {
	return &BadgerDBIndexedOperatorServiceController{
		logger:          logger,
		binder:          binder,
		indexedManager:  indexedManager,
		indexedOperator: indexedOperator,
	}
}
