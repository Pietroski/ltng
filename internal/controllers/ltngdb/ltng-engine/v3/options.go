package ltngdbcontrollerv3

import (
	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"

	ltngdbenginev3 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltngdbengine/v3"
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

func WithEngine(engine *ltngdbenginev3.LTNGEngine) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Controller); ok {
			c.engine = engine
		}
	}
}
