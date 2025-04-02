package ltngdb_engine_v1

import (
	"context"
	"fmt"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	"net"

	serializer_models "gitlab.com/pietroski-software-company/devex/golang/serializer/models"
	"gitlab.com/pietroski-software-company/devex/golang/transporthandler"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	ltng_engine_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v2"
	ltngdb_controller_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/controllers/ltngdb/ltng-engine/v2"
	ltngdb_factory_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/factories/ltngdb/gRPC/ltng-engine/v2"
	http_ltngdb_factory_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/factories/ltngdb/http/ltng-engine/v2"
)

func StartV2(
	ctx context.Context,
	cancelFn context.CancelFunc,
	cfg *ltng_node_config.Config,
	logger go_logger.Logger,
	s serializer_models.Serializer,
	binder go_binder.Binder,
	exiter func(code int),
) {
	logger.Debugf("opening ltngdb engine v2")
	engine, err := ltng_engine_v2.New(ctx)
	if err != nil {
		logger.Errorf(
			"failed to open ltngdb engine",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	controller, err := ltngdb_controller_v2.New(ctx,
		ltngdb_controller_v2.WithConfig(cfg),
		ltngdb_controller_v2.WithLogger(logger),
		ltngdb_controller_v2.WithBinder(binder),
		ltngdb_controller_v2.WithEngine(engine),
	)
	if err != nil {
		logger.Errorf(
			"error creating ltngdb controller",
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
			"failed creating net listener",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	httpListener, err := net.Listen(
		cfg.Node.Server.Network,
		fmt.Sprintf(":%v", cfg.Node.UI.Port),
	)
	if err != nil {
		logger.Errorf(
			"failed creating http net listener",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	factory, err := ltngdb_factory_v2.New(ctx,
		ltngdb_factory_v2.WithConfig(cfg),
		ltngdb_factory_v2.WithListener(listener),
		ltngdb_factory_v2.WithController(controller),
		ltngdb_factory_v2.WithEngine(engine),
	)
	if err != nil {
		logger.Errorf(
			"error creating ltngdb factory",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	httpFactory, err := http_ltngdb_factory_v2.New(ctx,
		http_ltngdb_factory_v2.WithConfig(cfg),
		http_ltngdb_factory_v2.WithLogger(logger),
		http_ltngdb_factory_v2.WithListener(httpListener),
	)
	if err != nil {
		logger.Errorf(
			"error initialising http ltngdb server factory",
			go_logger.Field{"error": err.Error()},
		)

		return
	}

	transporthandler.New(ctx, cancelFn,
		transporthandler.WithExiter(exiter),
		transporthandler.WithPprofServer(ctx),
		transporthandler.WithServers(transporthandler.ServerMapping{
			"lightning-node-server-engine-v2":    factory,
			"lightning-node-server-engine-v2-ui": httpFactory,
		}),
	).StartServers()
}
