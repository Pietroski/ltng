package badgerdb_manager_service_v3

import (
	"fmt"

	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	badgerdb_manager_adaptor_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/manager"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
)

type (
	BadgerDBManagerServiceV3Params struct {
		Logger  go_logger.Logger
		Binder  go_binder.Binder
		Manager badgerdb_manager_adaptor_v3.Manager
	}

	BadgerDBManagerServiceV3 struct {
		grpc_mngmt.UnimplementedManagementServer
		logger  go_logger.Logger
		binder  go_binder.Binder
		manager badgerdb_manager_adaptor_v3.Manager
	}
)

func NewBadgerDBManagerServiceV3(
	params *BadgerDBManagerServiceV3Params,
) (*BadgerDBManagerServiceV3, error) {
	if params == nil ||
		params.Logger == nil ||
		params.Binder == nil ||
		params.Manager == nil {
		return nil, fmt.Errorf("invalid params")
	}

	return &BadgerDBManagerServiceV3{
		logger:  params.Logger,
		binder:  params.Binder,
		manager: params.Manager,
	}, nil
}
