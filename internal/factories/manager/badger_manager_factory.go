package badgerdb_manager_factory

import (
	"context"
	"fmt"
	go_tracer_middleware "gitlab.com/pietroski-software-company/tools/middlewares/go-middlewares/pkg/tools/middlewares/gRPC/tracer"
	"net"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager"
	badgerdb_manager_controller "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/controllers/manager"
	grpc_mngmt "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/management"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_tracer "gitlab.com/pietroski-software-company/tools/tracer/go-tracer/v2/pkg/tools/tracer"
	"google.golang.org/grpc"
)

type (
	BadgerDBManagerServiceFactory struct {
		listener net.Listener
		server   *grpc.Server
		logger   go_logger.Logger
		tracer   go_tracer.Tracer
		binder   go_binder.Binder
		manager  manager.Manager
	}
)

func NewBadgerDBManagerService(
	listener net.Listener,
	logger go_logger.Logger,
	tracer go_tracer.Tracer,
	binder go_binder.Binder,
	manager manager.Manager,
) *BadgerDBManagerServiceFactory {
	factory := &BadgerDBManagerServiceFactory{
		listener: listener,
		logger:   logger,
		tracer:   tracer,
		binder:   binder,
		manager:  manager,
	}
	factory.handle()

	return factory
}

func (s *BadgerDBManagerServiceFactory) handle() {
	//var grpcOpts []grpc.ServerOption
	s.logger.Debugf("initialising manager server")

	grpcOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			func(
				ctx context.Context,
				req interface{},
				info *grpc.UnaryServerInfo,
				handler grpc.UnaryHandler,
			) (resp interface{}, err error) {
				ctx, err = go_tracer_middleware.GRPCServerTracer(ctx)
				if err != nil {
					err = fmt.Errorf("error tracing incoming request: %v", err)
					return nil, err
				}

				h, err := handler(ctx, req)
				return h, err
			},
		),
	}

	//var grpcOpts []grpc.ServerOption
	grpcServer := grpc.NewServer(grpcOpts...)

	grpc_mngmt.RegisterManagementServer(
		grpcServer,
		badgerdb_manager_controller.NewBadgerDBManagerServiceController(
			s.logger,
			s.binder,
			s.manager,
		),
	)

	s.server = grpcServer
}

func (s *BadgerDBManagerServiceFactory) Start() error {
	return s.server.Serve(s.listener)
}

func (s *BadgerDBManagerServiceFactory) Stop() {
	s.manager.ShutdownStores()
	s.manager.Shutdown()
	s.server.Stop()
}
