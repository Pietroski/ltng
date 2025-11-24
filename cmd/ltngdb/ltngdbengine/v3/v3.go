package cmdltngdbenginev3

import (
	"context"
	"fmt"
	"net"

	"gitlab.com/pietroski-software-company/golang/devex/options"
	serializermodels "gitlab.com/pietroski-software-company/golang/devex/serializer/models"
	"gitlab.com/pietroski-software-company/golang/devex/servermanager"
	"gitlab.com/pietroski-software-company/golang/devex/servermanager/pprofx"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"

	ltngdbenginev3 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltngdbengine/v3"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	ltngdbcontrollerv3 "gitlab.com/pietroski-software-company/lightning-db/internal/controllers/ltngdb/ltng-engine/v3"
	ltngdbfactoryv3 "gitlab.com/pietroski-software-company/lightning-db/internal/factories/ltngdb/gRPC/ltng-engine/v3"
	httpltngdbfactoryv3 "gitlab.com/pietroski-software-company/lightning-db/internal/factories/ltngdb/http/ltng-engine/v3"
)

func StartV3(
	ctx context.Context,
	cancelFn context.CancelFunc,
	cfg *ltng_node_config.Config,
	logger slogx.SLogger,
	_ serializermodels.Serializer,
	exiter func(code int),
	opts ...options.Option,
) {
	logger.Debug(ctx, "opening ltngdb engine v2")
	engine, err := ltngdbenginev3.New(ctx)
	if err != nil {
		logger.Error(ctx, "failed to open ltngdb engine", "error", err)

		return
	}

	controller, err := ltngdbcontrollerv3.New(ctx,
		ltngdbcontrollerv3.WithConfig(cfg),
		ltngdbcontrollerv3.WithLogger(logger),
		ltngdbcontrollerv3.WithEngine(engine),
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

	factory, err := ltngdbfactoryv3.New(ctx,
		ltngdbfactoryv3.WithConfig(cfg),
		ltngdbfactoryv3.WithListener(listener),
		ltngdbfactoryv3.WithController(controller),
		ltngdbfactoryv3.WithEngine(engine),
	)
	if err != nil {
		logger.Error(ctx, "error creating ltngdb factory", "error", err)

		return
	}

	httpFactory, err := httpltngdbfactoryv3.New(ctx,
		httpltngdbfactoryv3.WithConfig(cfg),
		httpltngdbfactoryv3.WithLogger(logger),
		httpltngdbfactoryv3.WithListener(httpListener),
	)
	if err != nil {
		logger.Error(ctx, "error creating http's ltngdb factory", "error", err)

		return
	}

	servermanager.New(ctx, cancelFn,
		servermanager.WithExiter(exiter),
		servermanager.WithLogger(logger),
		servermanager.WithPprofServer(ctx, append(opts, pprofx.WithPprofLogger(logger))...),
		servermanager.WithServers(servermanager.ServerMapping{
			"lightning-node-server-engine-v3":    factory,
			"lightning-node-server-engine-v3-ui": httpFactory,
		}),
	).StartServers()
}
