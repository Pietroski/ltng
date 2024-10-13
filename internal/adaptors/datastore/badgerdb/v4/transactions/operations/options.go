package badgerdb_operations_adaptor_v4

import (
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"

	badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/manager"
)

func WithManager(manager badgerdb_manager_adaptor_v4.Manager) options.Option {
	return func(cfg interface{}) {
		if c, ok := cfg.(*BadgerOperatorV4); ok {
			c.manager = manager
		}
	}
}

func WithSerializer(serializer go_serializer.Serializer) options.Option {
	return func(cfg interface{}) {
		if c, ok := cfg.(*BadgerOperatorV4); ok {
			c.serializer = serializer
		}
	}
}
