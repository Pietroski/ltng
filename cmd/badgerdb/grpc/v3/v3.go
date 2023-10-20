package v3

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

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/manager"
	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/transactions/operations"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/config"
	badgerdb_manager_factory "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/factories/manager"
	badgerdb_operator_factory "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/factories/operator"
	chainded_operator "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator"
)

type V3Params struct {
	ctx      context.Context
	cancelFn context.CancelFunc

	cfg *ltng_node_config.Config

	logger          go_logger.Logger
	serializer      go_serializer.Serializer
	binder          go_binder.Binder
	chainedOperator *chainded_operator.ChainOperator
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
		"badger-lightning-node-manager-v3":  mngrsvr,
		"badger-lightning-node-operator-v3": opsvr,
	})
}
