package badgerdb_operator_factory_v4

import (
	"context"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
	"google.golang.org/grpc/reflection"
	"net"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/reflection"

	go_tracer_middleware "gitlab.com/pietroski-software-company/tools/middlewares/go-middlewares/pkg/tools/middlewares/gRPC/tracer"

	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/config"
	badgerdb_operator_controller_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/controllers/badger/v4/operator"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
)

type (
	BadgerDBServiceOperatorFactoryV4 struct {
		listener net.Listener
		server   *grpc.Server
		cfg      *ltng_node_config.Config

		//manager  badgerdb_manager_adaptor_v4.Manager
		//operator badgerdb_operations_adaptor_v4.Operator

		controller *badgerdb_operator_controller_v4.BadgerDBOperatorServiceControllerV4
	}
)

func NewBadgerDBOperatorServiceFactoryV4(
	ctx context.Context,
	opts ...options.Option,
) (*BadgerDBServiceOperatorFactoryV4, error) {
	factory := &BadgerDBServiceOperatorFactoryV4{}
	options.ApplyOptions(factory, opts...)

	factory.handle()

	return factory, nil
}

func (s *BadgerDBServiceOperatorFactoryV4) handle() {
	grpcOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			go_tracer_middleware.NewGRPCUnaryTracerServerMiddleware(),
		),
	}
	grpcServer := grpc.NewServer(grpcOpts...)

	grpc_ops.RegisterOperationServer(grpcServer, s.controller)

	// Register reflection service on gRPC server.
	reflection.Register(grpcServer)

	s.server = grpcServer
}

func (s *BadgerDBServiceOperatorFactoryV4) Start() error {
	return s.server.Serve(s.listener)
}

func (s *BadgerDBServiceOperatorFactoryV4) Stop() {
	s.server.Stop()
}
