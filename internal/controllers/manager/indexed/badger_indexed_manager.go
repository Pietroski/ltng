package badgerdb_indexed_manager_controller

import (
	indexed_manager "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager/indexed"
	grpc_indexed_mngmnt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management/indexed/indexed_management"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
)

type (
	BadgerDBIndexedManagerServiceController struct {
		grpc_indexed_mngmnt.UnimplementedIndexedManagementServer
		logger         go_logger.Logger
		binder         go_binder.Binder
		indexedManager indexed_manager.IndexerManager
	}
)

func NewBadgerDBIndexerManagerServiceController(
	logger go_logger.Logger,
	binder go_binder.Binder,
	indexedManager indexed_manager.IndexerManager,
) *BadgerDBIndexedManagerServiceController {
	return &BadgerDBIndexedManagerServiceController{
		logger:         logger,
		binder:         binder,
		indexedManager: indexedManager,
	}
}
