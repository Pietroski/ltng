package v4

import (
	"github.com/dgraph-io/badger/v4"

	serializer_models "gitlab.com/pietroski-software-company/devex/golang/serializer/models"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
)

func WithManager(manager Manager) options.Option {
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

func WithDB(db *badger.DB) options.Option {
	return func(cfg interface{}) {
		if c, ok := cfg.(*BadgerLocalManagerV4); ok {
			c.db = db
		}
	}
}

func WithLogger(logger go_logger.Logger) options.Option {
	return func(cfg interface{}) {
		if c, ok := cfg.(*BadgerLocalManagerV4); ok {
			c.logger = logger
		}
	}
}
