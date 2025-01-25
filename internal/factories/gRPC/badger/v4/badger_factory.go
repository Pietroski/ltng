package badgerdb_factory_v4

import (
	"context"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	go_tracer_middleware "gitlab.com/pietroski-software-company/tools/middlewares/go-middlewares/pkg/tools/middlewares/gRPC/tracer"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4/manager"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config"
	badgerdb_controller_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/controllers/badger/v4"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

type (
	Factory struct {
		listener net.Listener
		cfg      *ltng_node_config.Config
		server   *grpc.Server

		manager    badgerdb_manager_adaptor_v4.Manager
		controller *badgerdb_controller_v4.Controller
	}
)

func New(
	ctx context.Context,
	opts ...options.Option,
) (*Factory, error) {
	factory := &Factory{}
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

	s.server = grpcServer
}

func (s *Factory) Start() error {
	return s.server.Serve(s.listener)
}

func (s *Factory) Stop() {
	s.manager.ShutdownStores()
	s.manager.Shutdown()
	s.server.Stop()
}
