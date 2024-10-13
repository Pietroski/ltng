package badgerdb_manager_adaptor_v4

import (
	"github.com/dgraph-io/badger/v4"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
)

func WithDB(db *badger.DB) options.Option {
	return func(cfg interface{}) {
		if c, ok := cfg.(*BadgerLocalManagerV4); ok {
			c.db = db
		}
	}
}

func WithSerializer(serializer go_serializer.Serializer) options.Option {
	return func(cfg interface{}) {
		if c, ok := cfg.(*BadgerLocalManagerV4); ok {
			c.serializer = serializer
		}
	}
}
