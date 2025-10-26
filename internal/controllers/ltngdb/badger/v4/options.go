package badgerdb_controller_v4

import (
	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4"
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

func WithManger(manager badgerdb_manager_adaptor_v4.Manager) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Controller); ok {
			c.manager = manager
		}
	}
}

func WithOperator(operator badgerdb_manager_adaptor_v4.Operator) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Controller); ok {
			c.operator = operator
		}
	}
}
