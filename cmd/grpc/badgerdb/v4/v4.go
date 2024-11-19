package badgerdb_engine_v4

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/dgraph-io/badger/v4"

	serializer_models "gitlab.com/pietroski-software-company/devex/golang/serializer/models"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	transporthandler "gitlab.com/pietroski-software-company/tools/transport-handler/go-transport-handler/v2/pkg/tools/handler"

	badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/manager"
	badgerdb_operations_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/transactions/operations"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/config"
	badgerdb_manager_controller_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/controllers/badger/v4/manager"
	badgerdb_operator_controller_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/controllers/badger/v4/operator"
	badgerdb_manager_factory_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/factories/badger/v4/manager"
	badgerdb_operator_factory_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/factories/badger/v4/operator"
)

func StartV4(
	ctx context.Context,
	cancelFn context.CancelFunc,
	cfg *ltng_node_config.Config,
	logger go_logger.Logger,
	s serializer_models.Serializer,
	binder go_binder.Binder,
) {
	logger.Debugf("opening badger local manager")
	db, err := badger.Open(badger.DefaultOptions(badgerdb_manager_adaptor_v4.InternalLocalManagement))
	if err != nil {
		logger.Errorf(
			"failed to open badger local manager",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	logger.Debugf("starting badger instances")
	mngr, err := badgerdb_manager_adaptor_v4.NewBadgerLocalManagerV4(ctx,
		badgerdb_manager_adaptor_v4.WithDB(db),
		badgerdb_manager_adaptor_v4.WithLogger(logger),
		badgerdb_manager_adaptor_v4.WithSerializer(s),
	)
	if err != nil {
		logger.Errorf(
			"failed to create NewBadgerLocalManagerV4",
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

	oprt, err := badgerdb_operations_adaptor_v4.NewBadgerOperatorV4(ctx,
		badgerdb_operations_adaptor_v4.WithSerializer(s),
		badgerdb_operations_adaptor_v4.WithManager(mngr),
	)
	if err != nil {
		logger.Errorf(
			"failed to create NewBadgerOperatorV4",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	managerController, err := badgerdb_manager_controller_v4.NewBadgerDBManagerServiceControllerV4(ctx,
		badgerdb_manager_controller_v4.WithConfig(cfg),
		badgerdb_manager_controller_v4.WithLogger(logger),
		badgerdb_manager_controller_v4.WithBinder(binder),
		badgerdb_manager_controller_v4.WithManger(mngr),
	)
	if err != nil {
		logger.Errorf(
			"ferror creating NewBadgerDBManagerServiceControllerV4",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	operatorController, err := badgerdb_operator_controller_v4.NewBadgerDBOperatorServiceControllerV4(ctx,
		badgerdb_operator_controller_v4.WithConfig(cfg),
		badgerdb_operator_controller_v4.WithLogger(logger),
		badgerdb_operator_controller_v4.WithBinder(binder),
		badgerdb_operator_controller_v4.WithManger(mngr),
		badgerdb_operator_controller_v4.WithOperator(oprt),
	)
	if err != nil {
		logger.Errorf(
			"ferror creating NewBadgerDBOperatorServiceControllerV4",
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
	mngrsvr, err := badgerdb_manager_factory_v4.NewBadgerDBManagerServiceFactoryV4(ctx,
		badgerdb_manager_factory_v4.WithConfig(cfg),
		badgerdb_manager_factory_v4.WithListener(managerListener),
		badgerdb_manager_factory_v4.WithManager(mngr),
		badgerdb_manager_factory_v4.WithController(managerController),
	)
	if err != nil {
		logger.Errorf(
			"failed to create NewBadgerDBManagerServiceFactoryV4",
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
	opsvr, err := badgerdb_operator_factory_v4.NewBadgerDBOperatorServiceFactoryV4(ctx,
		badgerdb_operator_factory_v4.WithConfig(cfg),
		badgerdb_operator_factory_v4.WithListener(operatorListener),
		//badgerdb_operator_factory_v4.WithManager(mngr),
		//badgerdb_operator_factory_v4.WithOperator(oprt),
		badgerdb_operator_factory_v4.WithController(operatorController),
	)
	if err != nil {
		logger.Errorf(
			"failed to create NewBadgerDBOperatorServiceFactoryV4",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	h := transporthandler.NewHandler(
		ctx, cancelFn, os.Exit, nil, logger, &transporthandler.Opts{Debug: true},
	)
	h.StartServers(transporthandler.ServerMapping{
		"badger-lightning-node-manager-v4":  mngrsvr,
		"badger-lightning-node-operator-v4": opsvr,
	})
}
