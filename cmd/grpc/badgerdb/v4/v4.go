package badgerdb_engine_v4

import (
	"context"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"net"

	serializer_models "gitlab.com/pietroski-software-company/devex/golang/serializer/models"
	"gitlab.com/pietroski-software-company/devex/golang/transporthandler"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4/manager"
	badgerdb_operations_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4/transactions/operations"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config"
	badgerdb_controller_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/controllers/badger/v4"
	"gitlab.com/pietroski-software-company/lightning-db/internal/factories/badger/v4"
)

func StartV4(
	ctx context.Context,
	cancelFn context.CancelFunc,
	cfg *ltng_node_config.Config,
	logger go_logger.Logger,
	s serializer_models.Serializer,
	binder go_binder.Binder,
	exitter func(code int),
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

	controller, err := badgerdb_controller_v4.New(ctx,
		badgerdb_controller_v4.WithConfig(cfg),
		badgerdb_controller_v4.WithLogger(logger),
		badgerdb_controller_v4.WithBinder(binder),
		badgerdb_controller_v4.WithManger(mngr),
		badgerdb_controller_v4.WithOperator(oprt),
	)
	if err != nil {
		logger.Errorf(
			"error creating BadgerDB v4",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	listener, err := net.Listen(
		cfg.Node.Server.Network,
		fmt.Sprintf(":%v", cfg.Node.Server.Port),
	)
	if err != nil {
		logger.Errorf(
			"failed to create net listener",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}
	factory, err := badgerdb_factory_v4.New(ctx,
		badgerdb_factory_v4.WithConfig(cfg),
		badgerdb_factory_v4.WithListener(listener),
		badgerdb_factory_v4.WithManager(mngr),
		badgerdb_factory_v4.WithController(controller),
	)
	if err != nil {
		logger.Errorf(
			"failed to create NewBadgerDBManagerServiceFactoryV4",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	transporthandler.New(ctx, cancelFn,
		transporthandler.WithExiter(exitter),
		transporthandler.WithServers(transporthandler.ServerMapping{
			"lightning-node-badger-db-engine-v4": factory,
		}),
	).StartServers()
}
