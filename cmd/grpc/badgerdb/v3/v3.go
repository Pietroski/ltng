package badgerdb_engine_v3

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/dgraph-io/badger/v3"

	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
	transporthandler "gitlab.com/pietroski-software-company/tools/transport-handler/go-transport-handler/v2/pkg/tools/handler"

	badgerdb_manager_adaptor_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/manager"
	badgerdb_operations_adaptor_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/transactions/operations"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/config"
	badgerdb_manager_factory_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/factories/badger/v3/manager"
	badgerdb_operator_factory_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/factories/badger/v3/operator"
	chainded_operator "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator"
)

type V3Params struct {
	Ctx      context.Context
	CancelFn context.CancelFunc

	Cfg *ltng_node_config.Config

	Logger          go_logger.Logger
	Serializer      go_serializer.Serializer
	Binder          go_binder.Binder
	ChainedOperator *chainded_operator.ChainOperator
}

func StartV3(
	ctx context.Context,
	cancelFn context.CancelFunc,
	cfg *ltng_node_config.Config,
	logger go_logger.Logger,
	serializer go_serializer.Serializer,
	binder go_binder.Binder,
	chainedOperator *chainded_operator.ChainOperator,
) {
	logger.Debugf("opening badger local manager")
	db, err := badger.Open(badger.DefaultOptions(badgerdb_manager_adaptor_v3.InternalLocalManagement))
	if err != nil {
		logger.Errorf(
			"failed to open badger local manager",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	logger.Debugf("starting badger instances")
	managerAdaptorParams := &badgerdb_manager_adaptor_v3.BadgerLocalManagerV3Params{
		DB:         db,
		Logger:     logger,
		Serializer: serializer,
	}
	mngr, err := badgerdb_manager_adaptor_v3.NewBadgerLocalManagerV3(managerAdaptorParams)
	if err != nil {
		logger.Errorf(
			"failed to create NewBadgerLocalManagerV3",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}
	if err = mngr.Start(); err != nil {
		logger.Errorf(
			"failed to start badger instances",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	operatorAdaptorParams := &badgerdb_operations_adaptor_v3.BadgerOperatorV3Params{
		Manager:         mngr,
		Serializer:      serializer,
		ChainedOperator: chainedOperator,
	}
	oprt, err := badgerdb_operations_adaptor_v3.NewBadgerOperatorV3(operatorAdaptorParams)
	if err != nil {
		logger.Errorf(
			"failed to create NewBadgerOperatorV3",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	managerListener, err := net.Listen(
		cfg.LTNGNode.LTNGManager.Network,
		fmt.Sprintf(":%v", cfg.LTNGNode.LTNGManager.Port),
	)
	if err != nil {
		logger.Errorf(
			"failed to create net listener",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}
	managerFactoryParams := &badgerdb_manager_factory_v3.BadgerDBManagerServiceFactoryV3Params{
		Listener: managerListener,
		Config:   cfg,
		Logger:   logger,
		Binder:   binder,
		Manager:  mngr,
	}
	mngrsvr, err := badgerdb_manager_factory_v3.NewBadgerDBManagerServiceFactoryV3(managerFactoryParams)
	if err != nil {
		logger.Errorf(
			"failed to create NewBadgerDBManagerServiceFactoryV3",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	operatorListener, err := net.Listen(
		cfg.LTNGNode.LTNGOperator.Network,
		fmt.Sprintf(":%v", cfg.LTNGNode.LTNGOperator.Port),
	)
	if err != nil {
		logger.Errorf(
			"failed to create net listener",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}
	operatorParams := &badgerdb_operator_factory_v3.BadgerDBServiceOperatorFactoryV3Params{
		Listener: operatorListener,
		Config:   cfg,
		Logger:   logger,
		Binder:   binder,
		Manager:  mngr,
		Operator: oprt,
	}
	opsvr, err := badgerdb_operator_factory_v3.NewBadgerDBOperatorServiceFactoryV3(operatorParams)
	if err != nil {
		logger.Errorf(
			"failed to create NewBadgerDBOperatorServiceFactoryV3",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	h := transporthandler.NewHandler(
		ctx, cancelFn, os.Exit, nil, logger, &transporthandler.Opts{Debug: true},
	)
	h.StartServers(transporthandler.ServerMapping{
		"badger-lightning-node-manager-v3":  mngrsvr,
		"badger-lightning-node-operator-v3": opsvr,
	})
}
