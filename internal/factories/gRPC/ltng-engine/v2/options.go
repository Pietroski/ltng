package ltngdb_factory_v2

import (
	"net"

	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	ltng_engine_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v2"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config"
	ltngdb_controller_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/controllers/ltng-engine/v2"
)

func WithConfig(config *ltng_node_config.Config) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Factory); ok {
			c.cfg = config
		}
	}
}

func WithListener(listener net.Listener) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Factory); ok {
			c.listener = listener
		}
	}
}

func WithEngine(
	engine *ltng_engine_v2.LTNGEngine,
) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Factory); ok {
			c.engine = engine
		}
	}
}

func WithController(
	controller *ltngdb_controller_v2.Controller,
) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Factory); ok {
			c.controller = controller
		}
	}
}
