package ltngdb_factory_v2

import (
	"context"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"gitlab.com/pietroski-software-company/golang/devex/options"
	go_tracer_middleware "gitlab.com/pietroski-software-company/tools/middlewares/go-middlewares/pkg/tools/middlewares/gRPC/tracer"

	ltng_engine_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v2"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	"gitlab.com/pietroski-software-company/lightning-db/internal/controllers/ltngdb/ltng-engine/v2"
	common_model "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

type (
	Factory struct {
		cfg *ltng_node_config.Config

		listener net.Listener
		server   *grpc.Server

		engine     *ltng_engine_v2.LTNGEngine
		controller *ltngdb_controller_v2.Controller
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
					Engine: common_model.LightningEngineV2EngineVersionType.String(),
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
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     15 * time.Second,
			MaxConnectionAge:      30 * time.Second,
			MaxConnectionAgeGrace: 5 * time.Second,
			Time:                  5 * time.Second,
			Timeout:               1 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}
	grpcServer := grpc.NewServer(grpcOpts...)
	grpc_ltngdb.RegisterLightningDBServer(grpcServer, s.controller)

	// Register reflection service on gRPC server.
	reflection.Register(grpcServer)

	//go func() {
	//	for {
	//		time.Sleep(time.Second)
	//		log.Printf("Server state: %v", grpcServer.GetServiceInfo())
	//	}
	//}()

	s.server = grpcServer
}

func (s *Factory) Start() error {
	return s.server.Serve(s.listener)
}

func (s *Factory) Stop() {
	s.server.GracefulStop()
	s.engine.Close()
}
