package v4

import (
	"github.com/dgraph-io/badger/v4"

	serializer_models "gitlab.com/pietroski-software-company/golang/devex/serializer/models"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
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

func WithLogger(logger slogx.SLogger) options.Option {
	return func(cfg interface{}) {
		if c, ok := cfg.(*BadgerLocalManagerV4); ok {
			c.logger = logger
		}
	}
}
