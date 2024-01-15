package badgerdb_operator_controller_v3

import (
	"fmt"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	badgerdb_manager_adaptor_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/manager"
	badgerdb_operations_adaptor_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/transactions/operations"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

type (
	BadgerDBOperatorServiceControllerV3Params struct {
		Logger   go_logger.Logger
		Binder   go_binder.Binder
		Manager  badgerdb_manager_adaptor_v3.Manager
		Operator badgerdb_operations_adaptor_v3.Operator
	}

	BadgerDBOperatorServiceControllerV3 struct {
		grpc_ops.UnimplementedOperationServer
		logger   go_logger.Logger
		binder   go_binder.Binder
		manager  badgerdb_manager_adaptor_v3.Manager
		operator badgerdb_operations_adaptor_v3.Operator
	}
)

func NewBadgerDBOperatorServiceControllerV3(
	params *BadgerDBOperatorServiceControllerV3Params,
) (*BadgerDBOperatorServiceControllerV3, error) {
	if params == nil ||
		params.Logger == nil ||
		params.Binder == nil ||
		params.Manager == nil ||
		params.Operator == nil {
		return nil, fmt.Errorf("invalid params")
	}

	return &BadgerDBOperatorServiceControllerV3{
		logger:   params.Logger,
		binder:   params.Binder,
		manager:  params.Manager,
		operator: params.Operator,
	}, nil
}
