package badgerdb_engine_v4

import (
	"context"
	"fmt"
	"net"

	"github.com/dgraph-io/badger/v4"

	serializer_models "gitlab.com/pietroski-software-company/golang/devex/serializer/models"
	"gitlab.com/pietroski-software-company/golang/devex/servermanager"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"

	"gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	badgerdb_controller_v5 "gitlab.com/pietroski-software-company/lightning-db/internal/controllers/ltngdb/badger/v4"
	"gitlab.com/pietroski-software-company/lightning-db/internal/factories/ltngdb/gRPC/badger/v4"
)

func StartV4(
	ctx context.Context,
	cancelFn context.CancelFunc,
	cfg *ltng_node_config.Config,
	logger slogx.SLogger,
	s serializer_models.Serializer,
	binder go_binder.Binder,
	exitter func(code int),
) {
	logger.Debug(ctx, "opening badger local manager")
	db, err := badger.Open(badger.DefaultOptions(v4.InternalLocalManagement))
	if err != nil {
		logger.Error(ctx, "opening badger local manager", err)

		return
	}

	logger.Debug(ctx, "starting badger instances")
	mngr, err := v4.NewBadgerLocalManagerV4(ctx,
		v4.WithDB(db),
		v4.WithLogger(logger),
		v4.WithSerializer(s),
	)
	if err != nil {
		logger.Error(ctx, "failed to create NewBadgerLocalManagerV4", "error", err)

		return
	}
	if err = mngr.Start(); err != nil {
		logger.Error(ctx, "failed to start badger instances", "error", err)

		return
	}

	oprt, err := v4.NewBadgerOperatorV4(ctx,
		v4.WithSerializer(s),
		v4.WithManager(mngr),
	)
	if err != nil {
		logger.Error(ctx, "failed to create NewBadgerOperatorV4", "error", err)

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
		logger.Error(ctx, "failed creating badgerdb v4", "error", err)

		return
	}

	listener, err := net.Listen(
		cfg.Node.Server.Network,
		fmt.Sprintf(":%v", cfg.Node.Server.Port),
	)
	if err != nil {
		logger.Error(ctx, "failed to create net listener", "error", err)

		return
	}
	factory, err := badgerdb_factory_v4.New(ctx,
		badgerdb_factory_v4.WithConfig(cfg),
		badgerdb_factory_v4.WithListener(listener),
		badgerdb_factory_v4.WithManager(mngr),
		badgerdb_factory_v4.WithController(controller),
	)
	if err != nil {
		logger.Error(ctx, "failed to create NewBadgerDBManagerServiceFactoryV4", "error", err)

		return
	}

	servermanager.New(ctx, cancelFn,
		servermanager.WithExiter(exitter),
		servermanager.WithPprofServer(ctx),
		servermanager.WithServers(servermanager.ServerMapping{
			"lightning-node-badger-db-engine-v4": factory,
		}),
	).StartServers()
}
