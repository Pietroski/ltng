package badgerdb_operations_adaptor_v4

import (
	serializer_models "gitlab.com/pietroski-software-company/devex/golang/serializer/models"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4/manager"
)

func WithManager(manager badgerdb_manager_adaptor_v4.Manager) options.Option {
	return func(cfg interface{}) {
		if c, ok := cfg.(*BadgerOperatorV4); ok {
			c.manager = manager
		}
	}
}

func WithSerializer(serializer serializer_models.Serializer) options.Option {
	return func(cfg interface{}) {
		if c, ok := cfg.(*BadgerOperatorV4); ok {
			c.serializer = serializer
		}
	}
}
