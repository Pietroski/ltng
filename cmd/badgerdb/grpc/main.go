package main

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/dgraph-io/badger/v3"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager"
	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/transactions/operations"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/config"
	badgerdb_manager_factory "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/factories/manager"
	badgerdb_operator_factory "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/factories/operator"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_env_extractor "gitlab.com/pietroski-software-company/tools/env-extractor/go-env-extractor/pkg/tools/env-extractor"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
	handlers_model "gitlab.com/pietroski-software-company/tools/transport-handler/go-transport-handler/v2/pkg/models/handlers"
	transporthandler "gitlab.com/pietroski-software-company/tools/transport-handler/go-transport-handler/v2/pkg/tools/handler"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"
)

func main() {
	ctx, cancelFn := context.WithCancel(context.Background())

	loggerPublishers := &go_logger.Publishers{}
	loggerOpts := &go_logger.Opts{
		Debug:   true,
		Publish: true,
	}
	logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts)

	cfg := &ltng_node_config.Config{}
	err := go_env_extractor.LoadEnvs(cfg)
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

	serializer := go_serializer.NewJsonSerializer()
	validator := go_validator.NewStructValidator()
	binder := go_binder.NewStructBinder(serializer, validator)

	logger.Debugf("starting badger instances")
	m := manager.NewBadgerLocalManager(db, serializer, logger)
	if err = m.Start(); err != nil {
		logger.Errorf(
			"failed to start badger instances",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	op := operations.NewBadgerOperator(
		m, serializer,
	)

	managerListener, err := net.Listen(
		cfg.LTNGNode.LTNGManager.Network,
		fmt.Sprintf(":%v", cfg.LTNGNode.LTNGManager.Address),
	)
	if err != nil {
		logger.Errorf(
			"failed to create net listener",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}
	mngrsvr := badgerdb_manager_factory.NewBadgerDBManagerService(
		managerListener, logger, binder, m,
	)

	operatorListener, err := net.Listen(
		cfg.LTNGNode.LTNGOperator.Network,
		fmt.Sprintf(":%v", cfg.LTNGNode.LTNGOperator.Address),
	)
	if err != nil {
		logger.Errorf(
			"failed to create net listener",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}
	opsvr := badgerdb_operator_factory.NewBadgerDBOperatorService(
		operatorListener, logger, binder, m, op,
	)

	h := transporthandler.NewHandler(
		ctx, cancelFn, os.Exit, nil, logger, &transporthandler.Opts{Debug: true},
	)
	h.StartServers(map[string]handlers_model.Server{
		"badger-lightning-node-manager":  mngrsvr,
		"badger-lightning-node-operator": opsvr,
	})
}
