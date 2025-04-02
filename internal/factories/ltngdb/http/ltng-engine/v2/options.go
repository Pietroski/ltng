package http_ltngdb_factory_v2

import (
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	"net"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
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

func WithLogger(logger go_logger.Logger) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Factory); ok {
			c.logger = logger
		}
	}
}
