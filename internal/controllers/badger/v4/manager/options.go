package badgerdb_manager_controller_v4

import (
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/manager"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/config"
)

func WithConfig(config *ltng_node_config.Config) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*BadgerDBManagerServiceControllerV4); ok {
			c.cfg = config
		}
	}
}

func WithLogger(logger go_logger.Logger) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*BadgerDBManagerServiceControllerV4); ok {
			c.logger = logger
		}
	}
}

func WithBinder(binder go_binder.Binder) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*BadgerDBManagerServiceControllerV4); ok {
			c.binder = binder
		}
	}
}

func WithManger(manager badgerdb_manager_adaptor_v4.Manager) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*BadgerDBManagerServiceControllerV4); ok {
			c.manager = manager
		}
	}
}
