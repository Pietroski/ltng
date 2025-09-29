package badgerdb_engine_v4

import (
	"context"
	"fmt"
	"net"

	"github.com/dgraph-io/badger/v4"

	serializer_models "gitlab.com/pietroski-software-company/devex/golang/serializer/models"
	"gitlab.com/pietroski-software-company/devex/golang/transporthandler"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	"gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	badgerdb_controller_v5 "gitlab.com/pietroski-software-company/lightning-db/internal/controllers/ltngdb/badger/v4"
	"gitlab.com/pietroski-software-company/lightning-db/internal/factories/ltngdb/gRPC/badger/v4"
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
	db, err := badger.Open(badger.DefaultOptions(v4.InternalLocalManagement))
	if err != nil {
		logger.Errorf(
			"failed to open badger local manager",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	logger.Debugf("starting badger instances")
	mngr, err := v4.NewBadgerLocalManagerV4(ctx,
		v4.WithDB(db),
		v4.WithLogger(logger),
		v4.WithSerializer(s),
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

	oprt, err := v4.NewBadgerOperatorV4(ctx,
		v4.WithSerializer(s),
		v4.WithManager(mngr),
	)
	if err != nil {
		logger.Errorf(
			"failed to create NewBadgerOperatorV4",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	controller, err := badgerdb_controller_v5.New(ctx,
		badgerdb_controller_v5.WithConfig(cfg),
		badgerdb_controller_v5.WithLogger(logger),
		badgerdb_controller_v5.WithBinder(binder),
		badgerdb_controller_v5.WithManger(mngr),
		badgerdb_controller_v5.WithOperator(oprt),
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
		transporthandler.WithPprofServer(ctx),
		transporthandler.WithServers(transporthandler.ServerMapping{
			"lightning-node-badger-db-engine-v4": factory,
		}),
	).StartServers()
}
