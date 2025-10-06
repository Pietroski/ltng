package v4

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"gitlab.com/pietroski-software-company/golang/devex/serializer"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	"gitlab.com/pietroski-software-company/golang/devex/tracer"

	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
)

func Test_Integration_CreateOpenStoreAndLoadIntoMemory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slogx.New(slogx.WithSLogLevel(slogx.LevelTest))

	logger.Test(ctx, "opening badger local manager")
	db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
	if err != nil {
		logger.Error(ctx, "failed to open badger local manager", "error", err)
	}

	s := serializer.NewJsonSerializer()

	logger.Test(ctx, "starting badger instances")
	m := &BadgerLocalManagerV4{
		db:            db,
		logger:        logger,
		serializer:    s,
		badgerMapping: &sync.Map{},
		reqMapping:    &sync.Map{},
	}
	err = m.Start()
	if err != nil {
		logger.Error(ctx, "failed to start badger instances", "error", err)
	}

	dnInfo := &badgerdb_management_models_v4.DBInfo{
		Name:         "badger-db-test-1",
		Path:         "test/path-1",
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Now(),
	}

	err = m.createOpenStoreAndLoadIntoMemory(dnInfo)
	require.NoError(t, err)

	rawRetrievedDBInfo, ok := m.badgerMapping.Load("badger-db-test-1")
	require.True(t, ok)
	retrievedDBInfo, ok := rawRetrievedDBInfo.(*badgerdb_management_models_v4.DBMemoryInfo)
	require.True(t, ok)
	t.Log(retrievedDBInfo)

	err = retrievedDBInfo.DB.Update(func(txn *badger.Txn) error {
		keyTest := []byte("test-key")
		valueTest := []byte("this is a value test")

		item, err := txn.Get(keyTest)
		if err == badger.ErrKeyNotFound {
			logger.Test(ctx, "key not found")
			return txn.Set(keyTest, valueTest)
		}

		if item != nil {
			logger.Test(ctx, "item is not nil")
			bs, err := item.ValueCopy(nil)
			require.NoError(t, err)
			str := string(bs)
			require.Equal(t, "this is a value test", str)
			return nil
		}

		return err
	})
	require.NoError(t, err)

	var fetchedVal []byte
	err = retrievedDBInfo.DB.View(func(txn *badger.Txn) error {
		keyTest := []byte("test-key")
		item, err := txn.Get(keyTest)
		if err != nil {
			logger.Test(ctx, "failed to get item from badger db", "err", err)
			return err
		}

		logger.Test(ctx, "item is not nil")
		fetchedVal, err = item.ValueCopy(nil)
		if err != nil {
			logger.Test(ctx, "failed to get value from badger db item", "err", err)
			return err
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []byte("this is a value test"), fetchedVal)
	logger.Debug(ctx, "successfully loaded data", "value", string(fetchedVal))

	// From here and below it only closes the database references to prevent memory leak
	m.badgerMapping.Range(func(key, value any) bool {
		dbInfo, ok := value.(*badgerdb_management_models_v4.DBMemoryInfo)
		if !ok {
			logger.Error(ctx, "corrupted stored memory", "key", key, "value", value)
		}

		err := dbInfo.DB.Close()
		if err != nil {
			logger.Error(ctx, "failed to close badger local manager", "db_info", dbInfo, "error", err)
		}

		m.badgerMapping.Delete(key)

		return true
	})
	m.Shutdown()

	m.badgerMapping.Range(
		func(key, value any) bool {
			logger.Test(ctx, "key is not nil", "key", key, "value", value)

			return true
		},
	)

	logger.Test(ctx, "cleaned up all successfully")
}

func Test_Integration_GetStoreInfoFromMemoryOrFromDisk(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slogx.New(slogx.WithSLogLevel(slogx.LevelTest))

	logger.Test(ctx, "opening badger local manager")
	db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
	if err != nil {
		logger.Error(ctx, "failed to open badger local manager", "error", err)
	}

	s := serializer.NewJsonSerializer()

	logger.Test(ctx, "starting badger instances")
	//m := NewBadgerLocalManagerV4(db, s)
	m := &BadgerLocalManagerV4{
		db:            db,
		serializer:    s,
		badgerMapping: &sync.Map{},
		logger:        logger,
		reqMapping:    &sync.Map{},
	}
	err = m.Start()
	if err != nil {
		logger.Error(ctx, "failed to start badger instances", "error", err)
	}

	dbInfo := &badgerdb_management_models_v4.DBInfo{
		Name:         "badger-db-test-2",
		Path:         "test/path-2",
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Now(),
	}

	err = m.createOpenStoreAndLoadIntoMemory(dbInfo)
	require.NoError(t, err)

	rawRetrievedDBInfo, ok := m.badgerMapping.Load("badger-db-test-2")
	require.True(t, ok)
	retrievedDBInfo, ok := rawRetrievedDBInfo.(*badgerdb_management_models_v4.DBMemoryInfo)
	require.True(t, ok)
	t.Log(retrievedDBInfo)

	memoryInfo, err := m.getStoreInfoFromMemoryOrFromDisk(ctx, dbInfo.Name)
	require.NoError(t, err)
	require.Equal(t, dbInfo.Name, memoryInfo.Name)
	require.Equal(t, dbInfo.Path, memoryInfo.Path)
	require.IsType(t, &badgerdb_management_models_v4.DBInfo{}, memoryInfo)
	//reflect.TypeOf(memoryInfo).ConvertibleTo(reflect.TypeOf(badgerdb_management_models_v4.DBInfo{}))

	m.ShutdownStores()
	m.Shutdown()

	m.badgerMapping.Range(
		func(key, value any) bool {
			logger.Test(ctx, "key is not nil", "key", key, "value", value)

			return true
		},
	)

	logger.Test(ctx, "cleaned up all successfully")
}

func Test_Integration_GetStoreMemoryInfoFromMemoryOrDisk(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slogx.New(slogx.WithSLogLevel(slogx.LevelTest))

	logger.Test(ctx, "opening badger local manager")
	db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
	if err != nil {
		logger.Error(ctx, "failed to open badger local manager", "error", err)
	}

	s := serializer.NewJsonSerializer()

	logger.Test(ctx, "starting badger instances")
	//m := NewBadgerLocalManagerV4(db, s)
	m := &BadgerLocalManagerV4{
		db:            db,
		serializer:    s,
		badgerMapping: &sync.Map{},
		logger:        logger,
		reqMapping:    &sync.Map{},
	}
	err = m.Start()
	if err != nil {
		logger.Error(ctx, "failed to start badger instances", "error", err)
	}

	dbInfo := &badgerdb_management_models_v4.DBInfo{
		Name:         "badger-db-test-3",
		Path:         "test/path-3",
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Now(),
	}

	err = m.createOpenStoreAndLoadIntoMemory(dbInfo)
	require.NoError(t, err)

	rawRetrievedDBInfo, ok := m.badgerMapping.Load("badger-db-test-3")
	require.True(t, ok)
	retrievedDBInfo, ok := rawRetrievedDBInfo.(*badgerdb_management_models_v4.DBMemoryInfo)
	require.True(t, ok)
	t.Log(retrievedDBInfo)

	memoryInfo, err := m.getStoreMemoryInfoFromMemoryOrDisk(ctx, dbInfo.Name)
	require.NoError(t, err)
	require.Equal(t, dbInfo.Name, memoryInfo.Name)
	require.Equal(t, dbInfo.Path, memoryInfo.Path)

	m.ShutdownStores()
	m.Shutdown()

	m.badgerMapping.Range(
		func(key, value any) bool {
			logger.Test(ctx, "key is not nil", "key", key, "value", value)

			return true
		},
	)

	logger.Test(ctx, "cleaned up all successfully")
}

func Test_Integration_DeleteFromMemoryAndDisk(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slogx.New(slogx.WithSLogLevel(slogx.LevelTest))

	logger.Test(ctx, "opening badger local manager")
	db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
	if err != nil {
		logger.Error(ctx, "failed to open badger local manager", "error", err)
	}

	s := serializer.NewJsonSerializer()

	logger.Test(ctx, "starting badger instances")
	//m := NewBadgerLocalManagerV4(db, s)
	m := &BadgerLocalManagerV4{
		db:            db,
		serializer:    s,
		badgerMapping: &sync.Map{},
		logger:        logger,
		reqMapping:    &sync.Map{},
	}
	err = m.Start()
	if err != nil {
		logger.Error(ctx, "failed to start badger instances", "error", err)
	}

	dbInfo := &badgerdb_management_models_v4.DBInfo{
		Name:         "badger-db-test-4",
		Path:         "test/path-4",
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Now(),
	}

	err = m.createOpenStoreAndLoadIntoMemory(dbInfo)
	require.NoError(t, err)

	rawRetrievedDBInfo, ok := m.badgerMapping.Load("badger-db-test-4")
	require.True(t, ok)
	retrievedDBInfo, ok := rawRetrievedDBInfo.(*badgerdb_management_models_v4.DBMemoryInfo)
	require.True(t, ok)
	t.Log(retrievedDBInfo)

	memoryInfo, err := m.getStoreMemoryInfoFromMemoryOrDisk(ctx, dbInfo.Name)
	require.NoError(t, err)
	require.Equal(t, dbInfo.Name, memoryInfo.Name)
	require.Equal(t, dbInfo.Path, memoryInfo.Path)

	err = m.deleteFromMemoryAndDisk(ctx, "badger-db-test-4")
	require.NoError(t, err)

	m.badgerMapping.Range(
		func(key, value any) bool {
			if key == "badger-db-test-4" {
				logger.Test(ctx, "key is nil", "key", key, "value", value)

				return false
			}

			return true
		},
	)

	m.ShutdownStores()
	m.Shutdown()

	m.badgerMapping.Range(
		func(key, value any) bool {
			logger.Test(ctx, "key is nil", "key", key, "value", value)

			return true
		},
	)

	logger.Test(ctx, "cleaned up all successfully")
}

func Test_Integration_Restart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slogx.New(slogx.WithSLogLevel(slogx.LevelTest))

	logger.Test(ctx, "opening badger local manager")
	db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
	if err != nil {
		logger.Error(ctx, "failed to open badger local manager", "error", err)
	}

	s := serializer.NewJsonSerializer()

	logger.Test(ctx, "starting badger instances")
	//m := NewBadgerLocalManagerV4(db, s)
	m := &BadgerLocalManagerV4{
		db:            db,
		serializer:    s,
		badgerMapping: &sync.Map{},
		logger:        logger,
		reqMapping:    &sync.Map{},
	}

	err = m.Start()
	if err != nil {
		logger.Error(ctx, "failed to start badger instances", "error", err)
	}

	logger.Test(ctx, "restarting badger instances")
	err = m.Restart()
	if err != nil {
		logger.Error(ctx, "failed to restart badger instances", "error", err)
	}

	logger.Test(ctx, "checking badger instances")
	m.badgerMapping.Range(
		func(key, value any) bool {
			logger.Test(ctx, "key is not nil", "key", key, "value", value)

			return true
		},
	)

	logger.Test(ctx, "shutting down badger instances and badger manager")
	m.ShutdownStores()
	m.Shutdown()

	logger.Test(ctx, "checking badger instances and badger manager")
	m.badgerMapping.Range(
		func(key, value any) bool {
			logger.Test(ctx, "key is not nil", "key", key, "value", value)

			return true
		},
	)

	logger.Test(ctx, "cleaned up all successfully")
}

func Test_Raw_Badger_Iterator_Behaviour(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slogx.New(slogx.WithSLogLevel(slogx.LevelTest))

	logger.Test(ctx, "opening badger db instance")
	db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
	if err != nil {
		logger.Error(ctx, "failed to open badger local manager", "error", err)
	}

	s := serializer.NewJsonSerializer()

	logger.Test(ctx, "starting badger instances")
	m := &BadgerLocalManagerV4{
		db:            db,
		serializer:    s,
		badgerMapping: &sync.Map{},
		logger:        logger,
		reqMapping:    &sync.Map{},
	}

	for i := 10; i <= 30; i++ {
		dnInfo := &badgerdb_management_models_v4.DBInfo{
			Name:         fmt.Sprintf("badger-db-test-%d", i),
			Path:         fmt.Sprintf("test/path-%v", i),
			CreatedAt:    time.Now(),
			LastOpenedAt: time.Now(),
		}

		err = m.createOpenStoreAndLoadIntoMemory(dnInfo)
		require.NoError(t, err)
	}
	m.ShutdownStores()

	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 5 // TODO: make it a const - scope yet to be defined.

	err = m.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			key, value, err := m.deserializeItem(item)
			if err != nil {
				log.Printf("failed to deserialize item: %v", err)
				continue
			}

			logger.Test(ctx, "badger instance", "name", key, "info", value)

			// Jump n - 1 from PrefetchSize totaling PrefetchSize number
			//if it.Valid() {
			//	it.Next()
			//	if it.Valid() {
			//		it.Next()
			//		if it.Valid() {
			//			it.Next()
			//			if it.Valid() {
			//				it.Next()
			//			}
			//		}
			//	}
			//}
		}

		return nil
	})
	require.NoError(t, err)

	s1, s2 := m.db.Size()
	logger.Test(ctx, "batch counters",
		"batch_count", m.db.MaxBatchCount(),
		"batch_size", m.db.MaxBatchSize(),
		"size", fmt.Sprintf("%v - %v", s1, s2))

	m.ShutdownStores()
	m.Shutdown()

	m.badgerMapping.Range(
		func(key, value any) bool {
			logger.Test(ctx, "key is not nil", "key", key, "value", value)

			return true
		},
	)

	logger.Test(ctx, "cleaned up all successfully")
}

func Test_Integration_ListStoreInfoFromMemoryOrDisk(t *testing.T) {
	t.Run(
		"list with pagination",
		func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			logger := slogx.New(slogx.WithSLogLevel(slogx.LevelTest))

			logger.Test(ctx, "opening badger db instance")
			db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
			if err != nil {
				logger.Error(ctx, "failed to open badger local manager", "error", err)
			}

			s := serializer.NewJsonSerializer()

			logger.Test(ctx, "starting badger instances")
			m := &BadgerLocalManagerV4{
				db:            db,
				serializer:    s,
				badgerMapping: &sync.Map{},
				logger:        logger,
				reqMapping:    &sync.Map{},
			}

			opt := badger.DefaultIteratorOptions
			opt.PrefetchSize = 5 // TODO: make it a const - scope yet to be defined.

			err = m.db.View(func(txn *badger.Txn) error {
				it := txn.NewIterator(opt)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()

					key, value, err := m.deserializeItem(item)
					if err != nil {
						log.Printf("failed to deserialize item: %v", err)
						continue
					}

					logger.Test(ctx, "badger instance", "name", key, "info", value)
				}

				return nil
			})
			require.NoError(t, err)

			logger.Test(ctx, "###########################################################################################")

			list, err := m.ListStoreInfo(ctx, 7, 4)
			require.NoError(t, err)

			logger.Test(ctx, "paginated list", "list", list)

			m.ShutdownStores()
			m.Shutdown()

			m.badgerMapping.Range(
				func(key, value any) bool {
					logger.Test(ctx, "key is not nil", "key", key, "value", value)

					return true
				},
			)

			logger.Test(ctx, "cleaned up all successfully")
		},
	)

	t.Run(
		"list with no pagination",
		func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			logger := slogx.New(slogx.WithSLogLevel(slogx.LevelTest))

			logger.Test(ctx, "opening badger db instance")
			db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
			if err != nil {
				logger.Error(ctx, "failed to open badger local manager", "err", err)
			}

			s := serializer.NewJsonSerializer()

			logger.Test(ctx, "starting badger instances")
			m := &BadgerLocalManagerV4{
				db:            db,
				serializer:    s,
				badgerMapping: &sync.Map{},
				logger:        logger,
				reqMapping:    &sync.Map{},
			}

			opt := badger.DefaultIteratorOptions
			opt.PrefetchSize = 5 // TODO: make it a const - scope yet to be defined.

			err = m.db.View(func(txn *badger.Txn) error {
				it := txn.NewIterator(opt)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					item := it.Item()

					key, value, err := m.deserializeItem(item)
					if err != nil {
						log.Printf("failed to deserialize item: %v", err)
						continue
					}

					logger.Test(ctx, "badger instance", "name", key, "info", value)
				}

				return nil
			})
			require.NoError(t, err)

			logger.Test(ctx, "###########################################################################################")

			list, err := m.ListStoreInfo(ctx, 0, 0)
			require.NoError(t, err)

			logger.Test(ctx, "not paginated list - list all", "list", list)

			m.ShutdownStores()
			m.Shutdown()

			m.badgerMapping.Range(
				func(key, value any) bool {
					logger.Test(ctx, "key is not nil", "key", key, "value", value)

					return true
				},
			)

			logger.Test(ctx, "cleaned up all successfully")
		},
	)
}

func Test_Integration_ListStoreMemoryInfoFromMemoryOrDisk(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slogx.New(slogx.WithSLogLevel(slogx.LevelTest))

	logger.Test(ctx, "opening badger db instance")
	db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
	if err != nil {
		logger.Error(ctx, "failed to open badger local manager", "err", err)
	}

	s := serializer.NewJsonSerializer()

	logger.Test(ctx, "starting badger instances")
	m := &BadgerLocalManagerV4{
		db:            db,
		serializer:    s,
		badgerMapping: &sync.Map{},
		logger:        logger,
		reqMapping:    &sync.Map{},
	}

	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 5 // TODO: make it a const - scope yet to be defined.

	err = m.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			key, value, err := m.deserializeItem(item)
			if err != nil {
				log.Printf("failed to deserialize item: %v", err)
				continue
			}

			logger.Test(ctx, "badger instance", "name", key, "info", value)
		}

		return nil
	})
	require.NoError(t, err)

	logger.Test(ctx, "###################################################################################################")

	list, err := m.ListStoreMemoryInfo(ctx, 7, 4)
	require.NoError(t, err)

	logger.Test(ctx, "paginated list", "list", list)

	m.ShutdownStores()
	m.Shutdown()

	m.badgerMapping.Range(
		func(key, value any) bool {
			logger.Test(ctx, "key is not nil", "key", key, "value", value)

			return true
		},
	)

	logger.Test(ctx, "cleaned up all successfully")
}

func Test_Integration_CreateStore(t *testing.T) {
	t.Run(
		"happy path",
		func(t *testing.T) {
			var err error

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			logger := slogx.New(slogx.WithSLogLevel(slogx.LevelTest))
			ctx, err = tracer.New().Trace(ctx)

			db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
			require.NoError(t, err)

			serializer := serializer.NewJsonSerializer()

			badgerManager, err := NewBadgerLocalManagerV4(ctx, WithDB(db), WithSerializer(serializer))
			require.NoError(t, err)

			err = badgerManager.Start()
			require.NoError(t, err)

			stores, err := badgerManager.ListStoreInfo(ctx, 0, 0)
			require.NoError(t, err)
			logger.Test(ctx, "stores", "store_list", stores)

			info := &badgerdb_management_models_v4.DBInfo{
				Name:         "integration-manager-test",
				Path:         "integration-manager-test/path-1",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}

			err = badgerManager.CreateStore(ctx, info)
			require.NoError(t, err)

			stores, err = badgerManager.ListStoreInfo(ctx, 0, 0)
			require.NoError(t, err)
			logger.Test(ctx, "stores", "store_list", stores)

			err = badgerManager.DeleteStore(ctx, info.Name)
			require.NoError(t, err)

			stores, err = badgerManager.ListStoreInfo(ctx, 0, 0)
			require.NoError(t, err)
			logger.Test(ctx, "stores", "store_list", stores)
		},
	)
}
