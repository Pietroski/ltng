package http_ltngdb_factory_v2

import (
	"context"
	"fmt"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	"net"
	"net/http"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	common_model "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/httpx"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

type (
	Factory struct {
		ctx    context.Context
		cfg    *ltng_node_config.Config
		logger go_logger.Logger

		listener  net.Listener
		server    *http.Server
		muxServer *runtime.ServeMux

		grpcServerAddress string
		httpServerAddress string
	}
)

func New(
	ctx context.Context,
	opts ...options.Option,
) (*Factory, error) {
	factory := &Factory{
		ctx: ctx,
		cfg: &ltng_node_config.Config{
			Node: &ltng_node_config.Node{
				Engine: &ltng_node_config.Engine{
					Engine: common_model.LightningEngineV2EngineVersionType.String(),
				},
				Server: &ltng_node_config.Server{
					Network: "tcp",
					Port:    "50050",
				},
				UI: &ltng_node_config.UI{
					Port: "7070",
				},
			},
		},
		grpcServerAddress: "127.0.0.1:50050",
		httpServerAddress: "127.0.0.1:7070",
	}
	options.ApplyOptions(factory, opts...)

	if err := factory.handle(); err != nil {
		return nil, err
	}

	return factory, nil
}

func (s *Factory) handle() error {
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             60 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	err := grpc_ltngdb.RegisterLightningDBHandlerFromEndpoint(s.ctx, mux, s.grpcServerAddress, opts)
	if err != nil {
		return fmt.Errorf("failed to RegisterLightningDBHandlerFromEndpoint: %v", err)
	}

	s.muxServer = mux

	return nil
}

func (s *Factory) Start() error {
	server := &http.Server{
		Addr:    s.httpServerAddress,
		Handler: httpx.EnableCors(s.muxServer),
	}
	s.server = server

	return s.server.Serve(s.listener)
}

func (s *Factory) Stop() {
	_ = s.listener.Close()
	err := s.server.Shutdown(s.ctx)
	if err != nil {
		s.logger.Errorf("failed to shutdown http gateway server",
			go_logger.Field{"error": err.Error()},
		)
	}
}
