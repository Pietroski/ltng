package ltngdb_controller_v2

import (
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	ltng_engine_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v2"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
)

func WithConfig(config *ltng_node_config.Config) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Controller); ok {
			c.cfg = config
		}
	}
}

func WithLogger(logger slogx.SLogger) options.Option {
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
