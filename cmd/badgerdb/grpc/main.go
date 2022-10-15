package main

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/dgraph-io/badger/v3"

	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_env_extractor "gitlab.com/pietroski-software-company/tools/env-extractor/go-env-extractor/pkg/tools/env-extractor"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
	transporthandler "gitlab.com/pietroski-software-company/tools/transport-handler/go-transport-handler/v2/pkg/tools/handler"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager"
	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/transactions/operations"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/config"
	badgerdb_manager_factory "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/factories/manager"
	badgerdb_operator_factory "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/factories/operator"
	chainded_operator "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator"
)

func main() {
	ctx, cancelFn := context.WithCancel(context.Background())

	var err error
	loggerPublishers := &go_logger.Publishers{}
	loggerOpts := &go_logger.Opts{
		Debug:   true,
		Publish: false,
	}
	logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts).FromCtx(ctx)

	serializer := go_serializer.NewJsonSerializer()
	validator := go_validator.NewStructValidator()
	binder := go_binder.NewStructBinder(serializer, validator)
	chainedOperator := chainded_operator.NewChainOperator()

	cfg := &ltng_node_config.Config{}
	err = go_env_extractor.LoadEnvs(cfg)
	if err != nil {
		logger.Errorf(
			"failed to load ltng node configs",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	logger.Debugf("opening badger local manager")
	db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
	if err != nil {
		logger.Errorf(
			"failed to open badger local manager",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	logger.Debugf("starting badger instances")
	mngr := manager.NewBadgerLocalManager(db, serializer, logger)
	if err = mngr.Start(); err != nil {
		logger.Errorf(
			"failed to start badger instances",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	oprt := operations.NewBadgerOperator(mngr, serializer, chainedOperator)

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
	mngrsvr := badgerdb_manager_factory.NewBadgerDBManagerService(
		managerListener, logger, binder, mngr,
	)

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
	opsvr := badgerdb_operator_factory.NewBadgerDBOperatorService(
		operatorListener, logger, binder, mngr, oprt,
	)

	h := transporthandler.NewHandler(
		ctx, cancelFn, os.Exit, nil, logger, &transporthandler.Opts{Debug: true},
	)
	h.StartServers(transporthandler.ServerMapping{
		"badger-lightning-node-manager":  mngrsvr,
		"badger-lightning-node-operator": opsvr,
	})
}
