package ltngdb_engine_v1

import (
	"context"
	"fmt"
	"net"

	serializermodels "gitlab.com/pietroski-software-company/golang/devex/serializer/models"
	"gitlab.com/pietroski-software-company/golang/devex/servermanager"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"

	ltng_engine_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v2"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	ltngdb_controller_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/controllers/ltngdb/ltng-engine/v2"
	ltngdb_factory_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/factories/ltngdb/gRPC/ltng-engine/v2"
	http_ltngdb_factory_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/factories/ltngdb/http/ltng-engine/v2"
)

func StartV2(
	ctx context.Context,
	cancelFn context.CancelFunc,
	cfg *ltng_node_config.Config,
	logger slogx.SLogger,
	_ serializermodels.Serializer,
	exiter func(code int),
) {
	logger.Debug(ctx, "opening ltngdb engine v2")
	engine, err := ltng_engine_v2.New(ctx)
	if err != nil {
		logger.Error(ctx, "failed to open ltngdb engine", "error", err)

		return
	}

	controller, err := ltngdb_controller_v2.New(ctx,
		ltngdb_controller_v2.WithConfig(cfg),
		ltngdb_controller_v2.WithLogger(logger),
		ltngdb_controller_v2.WithEngine(engine),
	)
	if err != nil {
		logger.Error(ctx, "error creating ltngdb controller", "error", err)

		return
	}

	listener, err := net.Listen(
		cfg.Node.Server.Network,
		fmt.Sprintf(":%v", cfg.Node.Server.Port),
	)
	if err != nil {
		logger.Error(ctx, "error creating net listener", "error", err)

		return
	}

	httpListener, err := net.Listen(
		cfg.Node.Server.Network,
		fmt.Sprintf(":%v", cfg.Node.UI.Port),
	)
	if err != nil {
		logger.Error(ctx, "error creating http net listener", "error", err)

		return
	}

	factory, err := ltngdb_factory_v2.New(ctx,
		ltngdb_factory_v2.WithConfig(cfg),
		ltngdb_factory_v2.WithListener(listener),
		ltngdb_factory_v2.WithController(controller),
		ltngdb_factory_v2.WithEngine(engine),
	)
	if err != nil {
		logger.Error(ctx, "error creating ltngdb factory", "error", err)

		return
	}

	httpFactory, err := http_ltngdb_factory_v2.New(ctx,
		http_ltngdb_factory_v2.WithConfig(cfg),
		http_ltngdb_factory_v2.WithLogger(logger),
		http_ltngdb_factory_v2.WithListener(httpListener),
	)
	if err != nil {
		logger.Error(ctx, "error creating http's ltngdb factory", "error", err)

		return
	}

	servermanager.New(ctx, cancelFn,
		servermanager.WithExiter(exiter),
		servermanager.WithLogger(logger),
		servermanager.WithPprofServer(ctx),
		servermanager.WithServers(servermanager.ServerMapping{
			"lightning-node-server-engine-v2":    factory,
			"lightning-node-server-engine-v2-ui": httpFactory,
		}),
	).StartServers()
}
