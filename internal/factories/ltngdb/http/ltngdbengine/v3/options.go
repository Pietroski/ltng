package httpltngdbfactoryv3

import (
	"net"

	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"

	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
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

func WithLogger(logger slogx.SLogger) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Factory); ok {
			c.logger = logger
		}
	}
}
