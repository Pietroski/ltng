package badgerdb_manager_controller

import (
	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/internal/adaptors/datastore/badgerdb/manager"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/schemas/generated/go/management"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
)

type (
	BadgerDBManagerServiceController struct {
		grpc_mngmt.UnimplementedManagementServer
		logger go_logger.Logger
		//serializer go_serializer.Serializer
		//validator  go_validator.Validator
		binder go_binder.Binder

		manager manager.Manager
	}
)

func NewBadgerDBServiceController(
	logger go_logger.Logger,
	//serializer go_serializer.Serializer,
	//validator go_validator.Validator,
	binder go_binder.Binder,

	manager manager.Manager,
) *BadgerDBManagerServiceController {
	return &BadgerDBManagerServiceController{
		logger: logger,
		//serializer: serializer,
		//validator:  validator,
		binder:  binder,
		manager: manager,
	}
}
