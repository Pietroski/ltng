package badgerdb_factory_v4

import (
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	"net"

	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4/manager"
	"gitlab.com/pietroski-software-company/lightning-db/internal/controllers/ltngdb/badger/v4"
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

func WithManager(
	manager badgerdb_manager_adaptor_v4.Manager,
) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Factory); ok {
			c.manager = manager
		}
	}
}

func WithController(
	controller *badgerdb_controller_v4.Controller,
) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Factory); ok {
			c.controller = controller
		}
	}
}
