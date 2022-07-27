package badgerdb_manager_controller

import (
	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
)

type (
	BadgerDBManagerServiceController struct {
		grpc_mngmt.UnimplementedManagementServer
		logger  go_logger.Logger
		binder  go_binder.Binder
		manager manager.Manager
	}
)

func NewBadgerDBManagerServiceController(
	logger go_logger.Logger,
	binder go_binder.Binder,
	manager manager.Manager,
) *BadgerDBManagerServiceController {
	return &BadgerDBManagerServiceController{
		logger:  logger,
		binder:  binder,
		manager: manager,
	}
}
