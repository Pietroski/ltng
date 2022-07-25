package badgerdb_operator_controller

import (
	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager"
	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/transactions/operations"

	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
)

type (
	BadgerDBOperatorServiceController struct {
		grpc_ops.UnimplementedOperationServer
		logger go_logger.Logger
		//serializer go_serializer.Serializer
		//validator  go_validator.Validator
		binder go_binder.Binder

		manager  manager.Manager
		operator operations.Operator
	}
)

func NewBadgerDBOperatorServiceController(
	logger go_logger.Logger,
	binder go_binder.Binder,
	manager manager.Manager,
	operator operations.Operator,
) *BadgerDBOperatorServiceController {
	return &BadgerDBOperatorServiceController{
		logger: logger,
		binder: binder,
		manager:  manager,
		operator: operator,
	}
}
