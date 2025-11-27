package ltngdbfactoryv3

import (
	"net"

	"gitlab.com/pietroski-software-company/golang/devex/options"

	ltngdbenginev3 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltngdbengine/v3"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	ltngdbcontrollerv3 "gitlab.com/pietroski-software-company/lightning-db/internal/controllers/ltngdb/ltngdbengine/v3"
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
	engine *ltngdbenginev3.LTNGEngine,
) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Factory); ok {
			c.engine = engine
		}
	}
}

func WithController(
	controller *ltngdbcontrollerv3.Controller,
) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Factory); ok {
			c.controller = controller
		}
	}
}
