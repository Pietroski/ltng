package manager

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
)

const (
	InternalLocalManagement = ".db/internal/local/management"

	ErrCorruptedStoreDataCastFailure = "corrupted stored memory - failed to cast into DBMemInfo model"
	ErrKeyNotFound                   = "key not found"
)

var (
	ErrUnimplemented = fmt.Errorf("unimplemented method")
)

type (
	Manager interface {
		CreateOpenStoreAndLoadIntoMemory(
			info *management_models.DBInfo,
		) error
		DeleteFromMemoryAndDisk(
			ctx context.Context,
			name string,
		) error

		GetStoreInfoFromMemoryOrFromDisk(
			ctx context.Context,
			name string,
		) (*management_models.DBInfo, error)
		GetStoreMemoryInfoFromMemoryOrDisk(
			ctx context.Context,
			name string,
		) (*management_models.DBMemoryInfo, error)

		GetDBInfo(
			ctx context.Context,
			name string,
		) (*management_models.DBInfo, error)
		GetDBMemoryInfo(
			ctx context.Context,
			name string,
		) (*management_models.DBMemoryInfo, error)

		ListStoreInfoFromMemoryOrDisk(
			ctx context.Context,
			size, page int,
		) ([]*management_models.DBInfo, error)
		ListStoreMemoryInfoFromMemoryOrDisk(
			ctx context.Context,
			size, page int,
		) ([]*management_models.DBMemoryInfo, error)

		ValidatePagination(size, page int) (bool, error)
		Paginate(
			it *badger.Iterator,
			size, page int,
		) *badger.Iterator

		Start() error
		Shutdown()
		ShutdownStores()
		Restart() error
		// MigrateStore
		// BackupStore
		// (Purge|Truncate)Store
	}

	BadgerLocalManager struct {
		db *badger.DB
		//TODO: remove logger and instead user error wrapper for returning
		logger        go_logger.Logger
		serializer    go_serializer.Serializer
		badgerMapping *sync.Map
	}
)

func NewBadgerLocalManager(
	db *badger.DB,
	serializer go_serializer.Serializer,
	logger go_logger.Logger,
) Manager {
	m := &BadgerLocalManager{
		db: db,

		serializer:    serializer,
		logger:        logger,
		badgerMapping: &sync.Map{},
	}

	return m
}

// CreateOpenStoreAndLoadIntoMemory checks if the give into key already exists in database,
// if it does not, it opens a db path and stores it in the db manager and loads it into memory for caching.
func (m *BadgerLocalManager) CreateOpenStoreAndLoadIntoMemory(info *management_models.DBInfo) error {
	sKey, err := m.serializer.Serialize(info.Name)
	if err != nil {
		return err
	}

	err = m.db.Update(func(txn *badger.Txn) error {
		_, err = txn.Get(sKey)
		if err == badger.ErrKeyNotFound {
			return m.persistInfo(txn, info, sKey)
		}

		return err
	})

	return err
}

func (m *BadgerLocalManager) GetDBInfo(
	ctx context.Context,
	name string,
) (info *management_models.DBInfo, err error) {
	return m.GetStoreInfoFromMemoryOrFromDisk(ctx, name)
}

func (m *BadgerLocalManager) GetDBMemoryInfo(
	ctx context.Context,
	name string,
) (info *management_models.DBMemoryInfo, err error) {
	return m.GetStoreMemoryInfoFromMemoryOrDisk(ctx, name)
}

// getStoreFromMemory will try to look for the given name key in the sync map,
// if it finds the corresponding value, it will try to cast it;
// for any failure regarding any previous steps an error will be returned.
func (m *BadgerLocalManager) getStoreFromMemory(
	name string,
) (*management_models.DBMemoryInfo, error) {
	var err error
	info := &management_models.DBMemoryInfo{}

	if infoFromName, ok := m.badgerMapping.Load(name); ok {
		info, ok = infoFromName.(*management_models.DBMemoryInfo)
		if !ok {
			err = fmt.Errorf(ErrCorruptedStoreDataCastFailure)
			return info, err
		}

		return info, err
	}

	return info, fmt.Errorf(ErrKeyNotFound)
}

// getStoreFromDB will try to look for the given name key in the db,
// the key will be serialized and then the search will take place.
// If it finds the corresponding value, it will try to deserialize the found item.
// For any failure regarding any previous steps an error will be returned.
func (m *BadgerLocalManager) getStoreFromDB(
	name string,
) (*management_models.DBInfo, error) {
	var err error
	info := &management_models.DBInfo{}

	serializedKey, err := m.serializer.Serialize(name)
	if err != nil {
		return info, err
	}

	if err = m.db.View(
		func(txn *badger.Txn) error {
			rawItem, err := txn.Get(serializedKey)
			if err != nil {
				return fmt.Errorf("failed to retrieve item from key; err: %v", err)
			}

			_, *info, err = m.deserializeItem(rawItem)
			if err != nil {
				return fmt.Errorf("failed to deserialize item; err: %v", err)
			}

			return nil
		},
	); err != nil {
		return info, err
	}

	return info, nil
}

func (m *BadgerLocalManager) GetStoreInfoFromMemoryOrFromDisk(
	ctx context.Context,
	name string,
) (info *management_models.DBInfo, err error) {
	logger := m.logger.FromCtx(ctx)

	if info, err := m.getStoreFromMemory(name); err == nil {
		return info.MemoryInfoToInfo(), err
	}

	logger.Warningf(
		"failed to get store from memory",
		go_logger.Mapper("err", err.Error()),
	)
	logger.Warningf("trying to get store from db")
	if info, err := m.getStoreFromDB(name); err == nil {
		return info, err
	}

	logger.Warningf(
		"failed to get store from db",
		go_logger.Mapper("err", err.Error()),
	)
	return info, fmt.Errorf("failed to get store from db")
}

func (m *BadgerLocalManager) GetStoreMemoryInfoFromMemoryOrDisk(
	ctx context.Context,
	name string,
) (*management_models.DBMemoryInfo, error) {
	logger := m.logger.FromCtx(ctx)
	dbMemoryInfo, err := m.getStoreFromMemory(name)
	if err == nil {
		return dbMemoryInfo, err
	}

	logger.Warningf(
		"failed to get store from memory",
		go_logger.Mapper("err", err.Error()),
	)
	logger.Warningf("trying to get store from db")
	dbInfo, err := m.getStoreFromDB(name)
	if err != nil {
		logger.Warningf(
			"failed to get store from db",
			go_logger.Mapper("err", err.Error()),
		)
		return &management_models.DBMemoryInfo{}, fmt.Errorf("failed to get store from db")
	}

	db, err := m.openLoadAndReturn(dbInfo)
	if err != nil {
		logger.Warningf(
			"failed to open store retrieved from db",
			go_logger.Mapper("err", err.Error()),
		)
		return &management_models.DBMemoryInfo{}, err
	}

	openedDBInfo := dbInfo.InfoToMemoryInfo(db)
	return openedDBInfo, nil
}

// DeleteFromMemoryAndDisk first checks the on memory sync map and
// if present, closes the running instance to the given database.
// It will then serialize the given key and search for it in the database,
// if found, it will delete it from the records.
// If the data is corrupted or the opened pointer is not able to be closed,
// it wil return an error.
// Also, if it fails to serialize, it will return an error.
// If by any case it does not find the key or if it fails by any reason to delete
// it will return an error.
func (m *BadgerLocalManager) DeleteFromMemoryAndDisk(
	ctx context.Context,
	name string,
) error {
	logger := m.logger.FromCtx(ctx)

	if info, err := m.getStoreFromMemory(name); err == nil {
		if err = info.DB.Close(); err != nil {
			return fmt.Errorf("error closing the database - name: %v - err: %v", name, err)
		}

		m.badgerMapping.Delete(name)
	} else if err != nil && err.Error() != fmt.Errorf(ErrKeyNotFound).Error() {
		logger.Errorf(
			"error finding db in sync map",
			go_logger.Field{
				"name":  name,
				"error": err.Error(),
			},
		)
	}

	serializedName, err := m.serializer.Serialize(name)
	if err != nil {
		return fmt.Errorf("error serializing name field - name: %v - err: %v", name, err)
	}
	err = m.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(serializedName)
	})

	return err
}

// ValidatePagination validates whether there is a valid pagination.
func (m *BadgerLocalManager) ValidatePagination(size, page int) (bool, error) {
	if size == 0 && page == 0 {
		return false, nil
	}

	if size == 0 && page != 0 || size != 0 && page == 0 {
		return false,
			fmt.Errorf(
				"invalid pagination - size and page must both be zero or different from zero",
			)
	}

	return true, nil
}

// Paginate traverses the iterator stack until
// the point to where it was requested.
func (m *BadgerLocalManager) Paginate(
	it *badger.Iterator,
	size, page int,
) *badger.Iterator {
	it.Rewind()
	for number := 1; number < page; number++ {
		for i := 0; i < size; i++ {
			if it.Valid() {
				it.Next()
			}
		}
	}

	return it
}

func (m *BadgerLocalManager) listAllStoresInfoFromMemoryOrDisk(
	ctx context.Context,
) ([]*management_models.DBInfo, error) {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 50

	var memoryInfoList []*management_models.DBInfo
	err := m.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			_, value, err := m.deserializeItem(item)
			if err != nil {
				log.Printf("failed to deserialize item: %v", err)
				continue
			}

			memoryInfoList = append(memoryInfoList, &value)
		}

		return nil
	})
	if err != nil {
		return memoryInfoList, fmt.Errorf("failed to obtain memory info list - err: %v", err)
	}

	return memoryInfoList, nil
}

func (m *BadgerLocalManager) listAllStoresMemoryInfoFromMemoryOrDisk(
	ctx context.Context,
) ([]*management_models.DBMemoryInfo, error) {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 50

	var memoryInfoList []*management_models.DBMemoryInfo
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

			if memoryInfoItem, err := m.getStoreFromMemory(key); err == nil {
				memoryInfoList = append(memoryInfoList, memoryInfoItem)

				continue
			}

			db, err := m.openLoadAndReturn(&value)
			if err != nil {
				log.Printf("failed to open and load item - %v: %v", key, err)
				continue
			}
			memoryInfoItem := value.InfoToMemoryInfo(db)
			memoryInfoList = append(memoryInfoList, memoryInfoItem)
		}

		return nil
	})
	if err != nil {
		return memoryInfoList, fmt.Errorf("failed to obtain memory info list - err: %v", err)
	}

	return memoryInfoList, nil
}

func (m *BadgerLocalManager) ListStoreInfoFromMemoryOrDisk(
	ctx context.Context,
	size, page int,
) ([]*management_models.DBInfo, error) {
	if ok, err := m.ValidatePagination(size, page); !ok {
		if err != nil {
			return []*management_models.DBInfo{}, err
		}

		return m.listAllStoresInfoFromMemoryOrDisk(ctx)
	}

	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = size

	memoryInfoList := make([]*management_models.DBInfo, size)
	err := m.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		it = m.Paginate(it, size, page)

		for idx := 0; idx < size; idx++ {
			if !it.Valid() {
				break
			}

			item := it.Item()

			_, value, err := m.deserializeItem(item)
			if err != nil {
				log.Printf("failed to deserialize item: %v", err)
				continue
			}

			memoryInfoList[idx] = &value

			it.Next()
		}

		return nil
	})
	if err != nil {
		return memoryInfoList, fmt.Errorf("failed to obtain memory info list - err: %v", err)
	}

	return memoryInfoList, nil
}

func (m *BadgerLocalManager) ListStoreMemoryInfoFromMemoryOrDisk(
	ctx context.Context,
	size, page int,
) ([]*management_models.DBMemoryInfo, error) {
	if ok, err := m.ValidatePagination(size, page); !ok {
		if err != nil {
			return []*management_models.DBMemoryInfo{}, err
		}

		return m.listAllStoresMemoryInfoFromMemoryOrDisk(ctx)
	}

	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = size * page

	memoryInfoList := make([]*management_models.DBMemoryInfo, size)
	err := m.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		it = m.Paginate(it, size, page)

		for idx := 0; idx < size; idx++ {
			if !it.Valid() {
				break
			}

			item := it.Item()

			key, value, err := m.deserializeItem(item)
			if err != nil {
				log.Printf("failed to deserialize item: %v", err)
				continue
			}

			if memoryInfoItem, err := m.getStoreFromMemory(key); err == nil {
				memoryInfoList[idx] = memoryInfoItem

				continue
			}

			db, err := m.openLoadAndReturn(&value)
			if err != nil {
				log.Printf("failed to open and load item - %v: %v", key, err)
				continue
			}
			memoryInfoItem := value.InfoToMemoryInfo(db)
			memoryInfoList[idx] = memoryInfoItem

			it.Next()
		}

		return nil
	})
	if err != nil {
		return memoryInfoList, fmt.Errorf("failed to obtain memory info list - err: %v", err)
	}

	return memoryInfoList, nil
}

// persistInfo serializes,
// stores info into badger,
// opens the database for the info's path,
// loads the memInfo into memory and,
// returns an error if any of the above steps fail.
func (m *BadgerLocalManager) persistInfo(
	txn *badger.Txn,
	info *management_models.DBInfo,
	sKey []byte,
) error {
	sValue, err := m.serializer.Serialize(info)
	if err != nil {
		return fmt.Errorf("failed to serialize info: %v", err)
	}

	if err = txn.Set(sKey, sValue); err != nil {
		return fmt.Errorf("failed to persist info in db: %v", err)
	}

	if err = m.openAndLoad(info); err != nil {
		return fmt.Errorf("failed to open and load info in memory: %v", err)
	}

	return nil
}

// openAndLoad opens the db path, and then,
// it loads into memory the opened db path information.
func (m *BadgerLocalManager) openAndLoad(
	info *management_models.DBInfo,
) error {
	path := InternalLocalManagement + "/" + info.Path
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return fmt.Errorf("error opening db path - %v: %v", info.Name, err)
	}
	info.LastOpenedAt = time.Now()

	memInfo := info.InfoToMemoryInfo(db)
	m.badgerMapping.Store(info.Name, memInfo)

	return nil
}

// openAndLoad opens the db path, then,
// it loads into memory the opened db path information,
// and finally return the pointer to the db;
// any previous mistake it returns the error.
func (m *BadgerLocalManager) openLoadAndReturn(
	info *management_models.DBInfo,
) (*badger.DB, error) {
	path := InternalLocalManagement + "/" + info.Path
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return db, fmt.Errorf("error opening db path - %v: %v", info.Name, err)
	}
	info.LastOpenedAt = time.Now()

	memInfo := info.InfoToMemoryInfo(db)
	m.badgerMapping.Store(info.Name, memInfo)

	return db, nil
}

// deserializeItem deserializes a badger item and,
// return its key as string, value as the DBInfo model and,
// and error in case of any previous step's failure.
func (m *BadgerLocalManager) deserializeItem(item *badger.Item) (
	key string,
	value management_models.DBInfo,
	err error,
) {
	serializedKey := item.KeyCopy(nil)
	serializedValue, err := item.ValueCopy(nil)
	if err != nil {
		return "",
			management_models.DBInfo{},
			fmt.Errorf("failed to retrieve serialized item value: %v", err)
	}

	if err = m.serializer.Deserialize(serializedKey, &key); err != nil {
		return "",
			management_models.DBInfo{},
			fmt.Errorf("failed to deserialize key on start: %v", err)
	}
	if err = m.serializer.Deserialize(serializedValue, &value); err != nil {
		return "",
			management_models.DBInfo{},
			fmt.Errorf("failed to deserialize value on start: %v", err)
	}

	return
}

// Start initializes all the database paths stored in the local manager.
// first it deserializes the item into key and value,
// secondly it opens the db path, and
// thirdly it loads into memory the opened db path information.
// Returns the loaded sync.map reference and,
// an error if any the above steps fail.
func (m *BadgerLocalManager) Start() error {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 10 // TODO: make it a const - scope yet to be defined.

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
func (m *BadgerLocalManager) Restart() error {
	logger := m.logger
	logger.Debugf("restarting badger-db manager")

	m.ShutdownStores()
	err := m.Start()

	return err
}

// Shutdown closes the connection from the badger-db manager.
func (m *BadgerLocalManager) Shutdown() {
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
func (m *BadgerLocalManager) ShutdownStores() {
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

		dbInfo, ok := value.(*management_models.DBMemoryInfo)
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
