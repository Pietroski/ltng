package badgerdb_manager_factory_v4

import (
	"net"

	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/manager"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/config"
	badgerdb_manager_controller_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/controllers/badger/v4/manager"
)

func WithConfig(config *ltng_node_config.Config) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*BadgerDBManagerServiceFactoryV4); ok {
			c.cfg = config
		}
	}
}

func WithListener(listener net.Listener) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*BadgerDBManagerServiceFactoryV4); ok {
			c.listener = listener
		}
	}
}

func WithManager(
	manager badgerdb_manager_adaptor_v4.Manager,
) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*BadgerDBManagerServiceFactoryV4); ok {
			c.manager = manager
		}
	}
}

func WithController(
	controller *badgerdb_manager_controller_v4.BadgerDBManagerServiceControllerV4,
) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*BadgerDBManagerServiceFactoryV4); ok {
			c.controller = controller
		}
	}
}
