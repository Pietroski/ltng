package badgerdb_manager_adaptor_v4

import (
	"log"
	"time"

	"github.com/dgraph-io/badger/v4"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
)

// Start initializes all the database paths stored in the local manager.
// first it deserializes the item into key and value,
// secondly it opens the db path, and
// thirdly it loads into memory the opened db path information.
// Returns the loaded sync.map reference and,
// an error if any the above steps fail.
func (m *BadgerLocalManagerV4) Start() error {
	opt := badger.DefaultIteratorOptions

	err := m.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			key, value, err := m.deserializeItem(item)
			if err != nil {
				log.Printf("failed to deserialize item: %v", err)
				continue
			}

			value.LastOpenedAt = time.Now()
			if err = m.updateLastOpenedAt(&value); err != nil {
				log.Printf("failed to update last opened time - %v: %v", key, err)
			}
			if err = m.openAndLoad(&value); err != nil {
				log.Printf("failed to open and load item - %v: %v", key, err)
			}
		}

		return nil
	})

	return err
}

// Restart closes all opened stores, then
// it starts again all the stored badger-db databases;
// if anything goes wrong on starting, it returns an error.
// The error on restarting is very likely to be the cause from
// an error that occurred on a fail trial to close one or more
// of the running stores.
// If a store did not close properly and somehow is still running,
// the start call will not be able to initialize all instances again.
// In that extreme scenario, before shutting down the whole service,
// try shutting down the stores and then the manager by hand, first.
func (m *BadgerLocalManagerV4) Restart() error {
	logger := m.logger
	logger.Debugf("restarting badger-db manager")

	m.ShutdownStores()
	err := m.Start()

	return err
}

// Shutdown closes the connection from the badger-db manager.
func (m *BadgerLocalManagerV4) Shutdown() {
	logger := m.logger
	logger.Debugf("closing badger-db manager")
	err := m.db.Close()
	if err != nil {
		logger.Errorf(
			"error shutting down badger-db manager",
			go_logger.Mapper("err", err.Error()),
		)
	}
}

// ShutdownStores closes the connections to all the stores
// allocated in memory on the sync map.
func (m *BadgerLocalManagerV4) ShutdownStores() {
	logger := m.logger
	logger.Debugf("closing badger-db instances")

	m.badgerMapping.Range(func(key, value any) bool {
		logger.Infof(
			"traversing sync map",
			go_logger.Field{
				"key":   key,
				"value": value,
			},
		)

		dbInfo, ok := value.(*badgerdb_management_models_v4.DBMemoryInfo)
		if !ok {
			logger.Errorf("corrupted stored memory")
		}

		err := dbInfo.DB.Close()
		if err != nil {
			logger.Errorf(
				"failed to close badger db instance",
				go_logger.Mapper("err", err.Error()),
				go_logger.Mapper("db_info", dbInfo),
			)
		}

		m.badgerMapping.Delete(key)

		return true
	})
}
