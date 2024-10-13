package badgerdb_manager_adaptor_v4

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/dgraph-io/badger/v4"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v4/management"
)

// CreateOpenStoreAndLoadIntoMemory method checks if the give into key already exists in database,
// if it does not, it opens a db path and stores it in the db manager and loads it into memory for caching.
func (m *BadgerLocalManagerV4) createOpenStoreAndLoadIntoMemory(
	info *badgerdb_management_models_v4.DBInfo,
) error {
	sKey, err := m.serializer.Serialize(info.Name)
	if err != nil {
		return err
	}

	{ // req mapping mtx
		_, ok := m.reqMapping.Load(info.Name)
		for ok {
			_, ok = m.reqMapping.Load(info.Name)
			runtime.Gosched()
		}
		m.reqMapping.Store(info.Name, info)
		defer m.reqMapping.Delete(info.Name)
	}

	err = m.db.Update(
		func(txn *badger.Txn) error {
			_, err = txn.Get(sKey)
			if err == badger.ErrKeyNotFound {
				return m.persistInfo(txn, info, sKey)
			}

			return err
		},
	)

	return err
}

// updateStoreInfo method updates store info into the database
func (m *BadgerLocalManagerV4) updateLastOpenedAt(info *badgerdb_management_models_v4.DBInfo) error {
	sKey, err := m.serializer.Serialize(info.Name)
	if err != nil {
		return err
	}

	err = m.db.Update(
		func(txn *badger.Txn) error {
			sValue, err := m.serializer.Serialize(info)
			if err != nil {
				return fmt.Errorf("failed to serialize info: %v", err)
			}

			if err = txn.Set(sKey, sValue); err != nil {
				return fmt.Errorf("failed to persist info in db: %v", err)
			}

			return nil
		},
	)

	return err
}

// persistInfo serializes,
// stores info into badger,
// opens the database for the info's path,
// loads the memInfo into memory and,
// returns an error if any of the above steps fail.
func (m *BadgerLocalManagerV4) persistInfo(
	txn *badger.Txn,
	info *badgerdb_management_models_v4.DBInfo,
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
func (m *BadgerLocalManagerV4) openAndLoad(
	info *badgerdb_management_models_v4.DBInfo,
) error {
	var memInfo *badgerdb_management_models_v4.DBMemoryInfo
	v, ok := m.badgerMapping.Load(info.Name)
	if ok {
		memInfo, ok = v.(*badgerdb_management_models_v4.DBMemoryInfo)
		if ok {
			return nil
		}
	}

	db, err := openBadger(info)
	if err != nil {
		return fmt.Errorf("error opening db path - %v: %v", info.Name, err)
	}

	memInfo = info.InfoToMemoryInfo(db)
	m.badgerMapping.Store(info.Name, memInfo)

	return nil
}

func openBadger(
	info *badgerdb_management_models_v4.DBInfo,
) (*badger.DB, error) {
	path := InternalLocalManagement + "/" + info.Path
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, fmt.Errorf("error opening db path - %v: %v", info.Name, err)
	}

	return db, nil
}

// openAndLoad opens the db path, then,
// it loads into memory the opened db path information,
// and finally return the pointer to the db;
// any previous mistake it returns the error.
func (m *BadgerLocalManagerV4) openLoadAndReturn(
	info *badgerdb_management_models_v4.DBInfo,
) (*badger.DB, error) {
	var memInfo *badgerdb_management_models_v4.DBMemoryInfo
	v, ok := m.badgerMapping.Load(info.Name)
	if ok {
		memInfo, ok = v.(*badgerdb_management_models_v4.DBMemoryInfo)
		if ok {
			return memInfo.DB, nil
		}
	}

	db, err := openBadger(info)
	if err != nil {
		return nil, fmt.Errorf("error opening db path - %v: %v", info.Name, err)
	}
	info.LastOpenedAt = time.Now()

	memInfo = info.InfoToMemoryInfo(db)
	m.badgerMapping.Store(info.Name, memInfo)

	return db, nil
}

// getStoreFromMemory will try to look for the given name key in the sync map,
// if it finds the corresponding value, it will try to cast it;
// for any failure regarding any previous steps an error will be returned.
func (m *BadgerLocalManagerV4) getStoreFromMemory(
	name string,
) (*badgerdb_management_models_v4.DBMemoryInfo, error) {
	var err error
	info := &badgerdb_management_models_v4.DBMemoryInfo{}

	if infoFromName, ok := m.badgerMapping.Load(name); ok {
		info, ok = infoFromName.(*badgerdb_management_models_v4.DBMemoryInfo)
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
func (m *BadgerLocalManagerV4) getStoreFromDB(
	name string,
) (*badgerdb_management_models_v4.DBInfo, error) {
	var err error
	info := &badgerdb_management_models_v4.DBInfo{}

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

func (m *BadgerLocalManagerV4) getStoreInfoFromMemoryOrFromDisk(
	ctx context.Context,
	name string,
) (info *badgerdb_management_models_v4.DBInfo, err error) {
	logger := m.logger.FromCtx(ctx)

	memoryInfo, err := m.getStoreFromMemory(name)
	if err == nil {
		return memoryInfo.MemoryInfoToInfo(), err
	}

	logger.Warningf(
		"failed to get store from memory",
		go_logger.Mapper("err", err.Error()),
	)
	logger.Warningf("trying to get store from db")
	if info, err = m.getStoreFromDB(name); err == nil {
		return info, err
	}

	logger.Warningf(
		"failed to get store from db",
		go_logger.Mapper("err", err.Error()),
	)
	return info, fmt.Errorf("failed to get store from db")
}

func (m *BadgerLocalManagerV4) getStoreMemoryInfoFromMemoryOrDisk(
	ctx context.Context,
	name string,
) (*badgerdb_management_models_v4.DBMemoryInfo, error) {
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
		return &badgerdb_management_models_v4.DBMemoryInfo{},
			fmt.Errorf("failed to get store from db")
	}

	db, err := m.openLoadAndReturn(dbInfo)
	if err != nil {
		logger.Warningf(
			"failed to open store retrieved from db",
			go_logger.Mapper("err", err.Error()),
		)
		return &badgerdb_management_models_v4.DBMemoryInfo{}, err
	}

	openedDBInfo := dbInfo.InfoToMemoryInfo(db)
	return openedDBInfo, nil
}

// deleteFromMemoryAndDisk first checks the on memory sync map and
// if present, closes the running instance to the given database.
// It will then serialize the given key and search for it in the database,
// if found, it will delete it from the records.
// If the data is corrupted or the opened pointer is not able to be closed,
// it wil return an error.
// Also, if it fails to serialize, it will return an error.
// If by any case it does not find the key or if it fails by any reason to delete
// it will return an error.
func (m *BadgerLocalManagerV4) deleteFromMemoryAndDisk(
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
	err = m.db.Update(
		func(txn *badger.Txn) error {
			return txn.Delete(serializedName)
		},
	)

	return err
}
