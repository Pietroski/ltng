package badgerdb_controller_v4

import (
	"context"

	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"

	"gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	common_model "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
)

type (
	Controller struct {
		grpc_ltngdb.UnimplementedLightningDBServer

		cfg      *ltng_node_config.Config
		logger   slogx.SLogger
		manager  v4.Manager
		operator v4.Operator
	}
)

func New(
	ctx context.Context,
	opts ...options.Option,
) (*Controller, error) {
	c := defaultControllerV4(ctx, opts...)
	return c, nil
}

func defaultControllerV4(
	ctx context.Context,
	opts ...options.Option,
) *Controller {
	c := &Controller{
		cfg: &ltng_node_config.Config{
			Node: &ltng_node_config.Node{
				Engine: &ltng_node_config.Engine{
					Engine: common_model.BadgerDBV4EngineVersionType.String(),
				},
				Server: &ltng_node_config.Server{
					Network: "tcp",
					Port:    "50050",
				},
			},
		},
		logger:   slogx.New(),
		manager:  nil,
		operator: nil,
	}
	options.ApplyOptions(c, opts...)

	return c
}
