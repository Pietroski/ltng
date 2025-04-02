package ltngdb_factory_v1

import (
	"context"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	go_tracer_middleware "gitlab.com/pietroski-software-company/tools/middlewares/go-middlewares/pkg/tools/middlewares/gRPC/tracer"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	ltng_engine_v1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v1"
	"gitlab.com/pietroski-software-company/lightning-db/internal/controllers/ltngdb/ltng-engine/v1"
	common_model "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

type (
	Factory struct {
		cfg *ltng_node_config.Config

		listener net.Listener
		server   *grpc.Server

		engine     *ltng_engine_v1.LTNGEngine
		controller *ltngdb_controller_v1.Controller
	}
)

func New(
	ctx context.Context,
	opts ...options.Option,
) (*Factory, error) {
	factory := &Factory{
		cfg: &ltng_node_config.Config{
			Node: &ltng_node_config.Node{
				Engine: &ltng_node_config.Engine{
					Engine: common_model.LightningEngineV1EngineVersionType.String(),
				},
				Server: &ltng_node_config.Server{
					Network: "tcp",
					Port:    "50050",
				},
			},
		},
	}
	options.ApplyOptions(factory, opts...)

	factory.handle()

	return factory, nil
}

func (s *Factory) handle() {
	grpcOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			go_tracer_middleware.NewGRPCUnaryTracerServerMiddleware(),
		),
	}
	grpcServer := grpc.NewServer(grpcOpts...)
	grpc_ltngdb.RegisterLightningDBServer(grpcServer, s.controller)

	// Register reflection service on gRPC server.
	reflection.Register(grpcServer)

	s.server = grpcServer
}

func (s *Factory) Start() error {
	return s.server.Serve(s.listener)
}

func (s *Factory) Stop() {
	s.engine.Close()
	_ = s.listener.Close()
	s.server.GracefulStop()
}
