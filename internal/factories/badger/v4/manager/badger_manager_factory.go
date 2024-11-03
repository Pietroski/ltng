package badgerdb_manager_factory_v4

import (
	"context"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	go_tracer_middleware "gitlab.com/pietroski-software-company/tools/middlewares/go-middlewares/pkg/tools/middlewares/gRPC/tracer"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/manager"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/config"
	badgerdb_manager_controller_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/controllers/badger/v4/manager"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
)

type (
	BadgerDBManagerServiceFactoryV4 struct {
		listener net.Listener
		cfg      *ltng_node_config.Config
		server   *grpc.Server

		manager    badgerdb_manager_adaptor_v4.Manager
		controller *badgerdb_manager_controller_v4.BadgerDBManagerServiceControllerV4
	}
)

func NewBadgerDBManagerServiceFactoryV4(
	ctx context.Context,
	opts ...options.Option,
) (*BadgerDBManagerServiceFactoryV4, error) {
	factory := &BadgerDBManagerServiceFactoryV4{}
	options.ApplyOptions(factory, opts...)

	factory.handle()

	return factory, nil
}

func (s *BadgerDBManagerServiceFactoryV4) handle() {
	grpcOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			go_tracer_middleware.NewGRPCUnaryTracerServerMiddleware(),
		),
	}
	grpcServer := grpc.NewServer(grpcOpts...)

	grpc_mngmt.RegisterManagementServer(grpcServer, s.controller)

	// Register reflection service on gRPC server.
	reflection.Register(grpcServer)

	s.server = grpcServer
}

func (s *BadgerDBManagerServiceFactoryV4) Start() error {
	return s.server.Serve(s.listener)
}

func (s *BadgerDBManagerServiceFactoryV4) Stop() {
	s.manager.ShutdownStores()
	s.manager.Shutdown()
	s.server.Stop()
}
