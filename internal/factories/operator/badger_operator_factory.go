package badgerdb_operator_factory

import (
	"context"
	"fmt"
	go_tracer_middleware "gitlab.com/pietroski-software-company/tools/middlewares/go-middlewares/pkg/tools/middlewares/gRPC/tracer"
	"net"

	"google.golang.org/grpc"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager"
	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/transactions/operations"
	badgerdb_operator_controller "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/controllers/operator"
	grpc_ops "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/schemas/generated/go/transactions/operations"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
)

type (
	BadgerDBServiceOperatorFactory struct {
		listener net.Listener
		server   *grpc.Server
		logger   go_logger.Logger
		//serializer go_serializer.Serializer
		//validator  go_validator.Validator
		binder go_binder.Binder

		manager  manager.Manager
		operator operations.Operator
	}
)

func NewBadgerDBOperatorService(
	listener net.Listener,
	logger go_logger.Logger,
//serializer go_serializer.Serializer,
//validator  go_validator.Validator,
	binder go_binder.Binder,

	manager manager.Manager,
	operator operations.Operator,
) *BadgerDBServiceOperatorFactory {
	factory := &BadgerDBServiceOperatorFactory{
		listener: listener,

		logger:   logger,
		binder:   binder,
		manager:  manager,
		operator: operator,
	}
	factory.handle()

	return factory
}

func (s *BadgerDBServiceOperatorFactory) handle() {
	//var grpcOpts []grpc.ServerOption

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

	grpcServer := grpc.NewServer(grpcOpts...)

	grpc_ops.RegisterOperationServer(
		grpcServer,
		badgerdb_operator_controller.NewBadgerDBOperatorServiceController(
			s.logger,
			s.binder,
			s.manager,
			s.operator,
		),
	)

	s.server = grpcServer
}

func (s *BadgerDBServiceOperatorFactory) Start() error {
	return s.server.Serve(s.listener)
}

func (s *BadgerDBServiceOperatorFactory) Stop() {
	s.server.Stop()
}
