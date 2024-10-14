package badgerdb_operator_factory_v4

import (
	"net"

	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/config"
	badgerdb_operator_controller_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/controllers/badger/v4/operator"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
)

func WithConfig(config *ltng_node_config.Config) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*BadgerDBServiceOperatorFactoryV4); ok {
			c.cfg = config
		}
	}
}

func WithListener(listener net.Listener) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*BadgerDBServiceOperatorFactoryV4); ok {
			c.listener = listener
		}
	}
}

//func WithManager(
//	manager badgerdb_manager_adaptor_v4.Manager,
//) options.Option {
//	return func(i interface{}) {
//		if c, ok := i.(*BadgerDBServiceOperatorFactoryV4); ok {
//			c.manager = manager
//		}
//	}
//}
//
//func WithOperator(
//	operator badgerdb_operations_adaptor_v4.Operator,
//) options.Option {
//	return func(i interface{}) {
//		if c, ok := i.(*BadgerDBServiceOperatorFactoryV4); ok {
//			c.operator = operator
//		}
//	}
//}

func WithController(
	controller *badgerdb_operator_controller_v4.BadgerDBOperatorServiceControllerV4,
) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*BadgerDBServiceOperatorFactoryV4); ok {
			c.controller = controller
		}
	}
}
