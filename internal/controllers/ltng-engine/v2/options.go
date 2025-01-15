package ltngdb_controller_v1

import (
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	ltng_engine_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v2"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config"
)

func WithConfig(config *ltng_node_config.Config) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Controller); ok {
			c.cfg = config
		}
	}
}

func WithLogger(logger go_logger.Logger) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Controller); ok {
			c.logger = logger
		}
	}
}

func WithBinder(binder go_binder.Binder) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Controller); ok {
			c.binder = binder
		}
	}
}

func WithEngine(engine *ltng_engine_v2.LTNGEngine) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Controller); ok {
			c.engine = engine
		}
	}
}
