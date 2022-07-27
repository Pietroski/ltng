package manager

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"
	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
)

func Test_Integration_CreateOpenStoreAndLoadIntoMemory(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	logger := go_logger.FromCtx(ctx)

	logger.Infof("opening badger local manager")
	db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
	if err != nil {
		logger.Errorf(
			"failed to open badger local manager",
			go_logger.Mapper("err", err.Error()),
		)
	}

	s := go_serializer.NewJsonSerializer()

	logger.Infof("starting badger instances")
	//m := manager.NewBadgerLocalManager(db, s)
	m := &BadgerLocalManager{
		db:            db,
		serializer:    s,
		badgerMapping: &sync.Map{},
		logger:        logger,
	}
	err = m.Start()
	if err != nil {
		logger.Errorf(
			"failed to start badger instances",
			go_logger.Mapper("err", err.Error()),
		)
	}

	dnInfo := &management_models.DBInfo{
		Name:         "badger-db-test-1",
		Path:         "test/path-1",
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Now(),
	}

	err = m.CreateOpenStoreAndLoadIntoMemory(dnInfo)
	require.NoError(t, err)

	rawRetrievedDBInfo, ok := m.badgerMapping.Load("badger-db-test-1")
	require.True(t, ok)
	retrievedDBInfo, ok := rawRetrievedDBInfo.(*management_models.DBMemoryInfo)
	require.True(t, ok)
	t.Log(retrievedDBInfo)

	err = retrievedDBInfo.DB.Update(func(txn *badger.Txn) error {
		keyTest := []byte("test-key")
		valueTest := []byte("this is a value test")

		item, err := txn.Get(keyTest)
		if err == badger.ErrKeyNotFound {
			logger.Debugf("key not found")
			return txn.Set(keyTest, valueTest)
		}

		if item != nil {
			logger.Debugf("item is not nil")
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
			return err
		}

		fetchedVal, err = item.ValueCopy(nil)
		return err
	})
	require.NoError(t, err)
	require.Equal(t, []byte("this is a value test"), fetchedVal)
	logger.Debugf(
		"fetched value ->",
		go_logger.Mapper("value", string(fetchedVal)),
	)

	// From here and below it only closes the database references to prevent memory leak
	m.badgerMapping.Range(func(key, value any) bool {
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
	m.Shutdown()

	m.badgerMapping.Range(
		func(key, value any) bool {
			// Should not be displayed
			logger.Warningf("remaining ->", map[string]interface{}{
				"key":   key,
				"value": value,
			})

			return true
		},
	)

	logger.Infof("cleaned up all successfully")
}

func Test_Integration_GetStoreInfoFromMemoryOrFromDisk(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	logger := go_logger.FromCtx(ctx)

	logger.Infof("opening badger local manager")
	db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
	if err != nil {
		logger.Errorf(
			"failed to open badger local manager",
			go_logger.Mapper("err", err.Error()),
		)
	}

	s := go_serializer.NewJsonSerializer()

	logger.Infof("starting badger instances")
	//m := manager.NewBadgerLocalManager(db, s)
	m := &BadgerLocalManager{
		db:            db,
		serializer:    s,
		badgerMapping: &sync.Map{},
		logger:        logger,
	}
	err = m.Start()
	if err != nil {
		logger.Errorf(
			"failed to start badger instances",
			go_logger.Mapper("err", err.Error()),
		)
	}

	dbInfo := &management_models.DBInfo{
		Name:         "badger-db-test-2",
		Path:         "test/path-2",
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Now(),
	}

	err = m.CreateOpenStoreAndLoadIntoMemory(dbInfo)
	require.NoError(t, err)

	rawRetrievedDBInfo, ok := m.badgerMapping.Load("badger-db-test-2")
	require.True(t, ok)
	retrievedDBInfo, ok := rawRetrievedDBInfo.(*management_models.DBMemoryInfo)
	require.True(t, ok)
	t.Log(retrievedDBInfo)

	memoryInfo, err := m.GetStoreInfoFromMemoryOrFromDisk(ctx, dbInfo.Name)
	require.NoError(t, err)
	require.Equal(t, dbInfo.Name, memoryInfo.Name)
	require.Equal(t, dbInfo.Path, memoryInfo.Path)
	require.IsType(t, &management_models.DBInfo{}, memoryInfo)
	//reflect.TypeOf(memoryInfo).ConvertibleTo(reflect.TypeOf(management_models.DBInfo{}))

	m.ShutdownStores()
	m.Shutdown()

	m.badgerMapping.Range(
		func(key, value any) bool {
			// Should not be displayed
			logger.Warningf("remaining ->", map[string]interface{}{
				"key":   key,
				"value": value,
			})

			return true
		},
	)

	logger.Infof("cleaned up all successfully")
}

func Test_Integration_GetStoreMemoryInfoFromMemoryOrDisk(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	logger := go_logger.FromCtx(ctx)

	logger.Infof("opening badger local manager")
	db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
	if err != nil {
		logger.Errorf(
			"failed to open badger local manager",
			go_logger.Mapper("err", err.Error()),
		)
	}

	s := go_serializer.NewJsonSerializer()

	logger.Infof("starting badger instances")
	//m := manager.NewBadgerLocalManager(db, s)
	m := &BadgerLocalManager{
		db:            db,
		serializer:    s,
		badgerMapping: &sync.Map{},
		logger:        logger,
	}
	err = m.Start()
	if err != nil {
		logger.Errorf(
			"failed to start badger instances",
			go_logger.Mapper("err", err.Error()),
		)
	}

	dbInfo := &management_models.DBInfo{
		Name:         "badger-db-test-3",
		Path:         "test/path-3",
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Now(),
	}

	err = m.CreateOpenStoreAndLoadIntoMemory(dbInfo)
	require.NoError(t, err)

	rawRetrievedDBInfo, ok := m.badgerMapping.Load("badger-db-test-3")
	require.True(t, ok)
	retrievedDBInfo, ok := rawRetrievedDBInfo.(*management_models.DBMemoryInfo)
	require.True(t, ok)
	t.Log(retrievedDBInfo)

	memoryInfo, err := m.GetStoreMemoryInfoFromMemoryOrDisk(ctx, dbInfo.Name)
	require.NoError(t, err)
	require.Equal(t, dbInfo.Name, memoryInfo.Name)
	require.Equal(t, dbInfo.Path, memoryInfo.Path)

	m.ShutdownStores()
	m.Shutdown()

	m.badgerMapping.Range(
		func(key, value any) bool {
			// Should not be displayed
			logger.Warningf("remaining ->", map[string]interface{}{
				"key":   key,
				"value": value,
			})

			return true
		},
	)

	logger.Infof("cleaned up all successfully")
}

func Test_Integration_DeleteFromMemoryAndDisk(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	logger := go_logger.FromCtx(ctx)

	logger.Infof("opening badger local manager")
	db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
	if err != nil {
		logger.Errorf(
			"failed to open badger local manager",
			go_logger.Mapper("err", err.Error()),
		)
	}

	s := go_serializer.NewJsonSerializer()

	logger.Infof("starting badger instances")
	//m := manager.NewBadgerLocalManager(db, s)
	m := &BadgerLocalManager{
		db:            db,
		serializer:    s,
		badgerMapping: &sync.Map{},
		logger:        logger,
	}
	err = m.Start()
	if err != nil {
		logger.Errorf(
			"failed to start badger instances",
			go_logger.Mapper("err", err.Error()),
		)
	}

	dbInfo := &management_models.DBInfo{
		Name:         "badger-db-test-4",
		Path:         "test/path-4",
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Now(),
	}

	err = m.CreateOpenStoreAndLoadIntoMemory(dbInfo)
	require.NoError(t, err)

	rawRetrievedDBInfo, ok := m.badgerMapping.Load("badger-db-test-4")
	require.True(t, ok)
	retrievedDBInfo, ok := rawRetrievedDBInfo.(*management_models.DBMemoryInfo)
	require.True(t, ok)
	t.Log(retrievedDBInfo)

	memoryInfo, err := m.GetStoreMemoryInfoFromMemoryOrDisk(ctx, dbInfo.Name)
	require.NoError(t, err)
	require.Equal(t, dbInfo.Name, memoryInfo.Name)
	require.Equal(t, dbInfo.Path, memoryInfo.Path)

	err = m.DeleteFromMemoryAndDisk(ctx, "badger-db-test-4")
	require.NoError(t, err)

	m.badgerMapping.Range(
		func(key, value any) bool {
			// Should not be displayed
			if key == "badger-db-test-4" {
				logger.Warningf("remaining ->", map[string]interface{}{
					"key":   key,
					"value": value,
				})

				return false
			}

			return true
		},
	)

	m.ShutdownStores()
	m.Shutdown()

	m.badgerMapping.Range(
		func(key, value any) bool {
			// Should not be displayed
			logger.Warningf("remaining ->", map[string]interface{}{
				"key":   key,
				"value": value,
			})

			return true
		},
	)

	logger.Infof("cleaned up all successfully")
}

func Test_Integration_Restart(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	logger := go_logger.FromCtx(ctx)

	logger.Infof("opening badger local manager")
	db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
	if err != nil {
		logger.Errorf(
			"failed to open badger local manager",
			go_logger.Mapper("err", err.Error()),
		)
	}

	s := go_serializer.NewJsonSerializer()

	logger.Infof("starting badger instances")
	//m := manager.NewBadgerLocalManager(db, s)
	m := &BadgerLocalManager{
		db:            db,
		serializer:    s,
		badgerMapping: &sync.Map{},
		logger:        logger,
	}

	err = m.Start()
	if err != nil {
		logger.Errorf(
			"failed to start badger instances",
			go_logger.Mapper("err", err.Error()),
		)
	}

	logger.Infof("restarting badger instances")
	err = m.Restart()
	if err != nil {
		logger.Errorf(
			"failed to restart badger instances",
			go_logger.Mapper("err", err.Error()),
		)
	}

	logger.Debugf("checking badger instances")
	m.badgerMapping.Range(
		func(key, value any) bool {
			logger.Warningf("remaining ->", map[string]interface{}{
				"key":   key,
				"value": value,
			})

			return true
		},
	)

	logger.Infof("shutting down badger instances and badger manager")
	m.ShutdownStores()
	m.Shutdown()

	logger.Infof("checking badger instances and badger manager")
	m.badgerMapping.Range(
		func(key, value any) bool {
			// Should not be displayed
			logger.Warningf("remaining ->", map[string]interface{}{
				"key":   key,
				"value": value,
			})

			return true
		},
	)

	logger.Infof("cleaned up all successfully")
}

func Test_Raw_Badger_Iterator_Behaviour(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	logger := go_logger.FromCtx(ctx)

	logger.Infof("opening badger db instance")
	db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
	if err != nil {
		logger.Errorf(
			"failed to open badger local manager",
			go_logger.Mapper("err", err.Error()),
		)
	}

	s := go_serializer.NewJsonSerializer()

	logger.Infof("starting badger instances")
	m := &BadgerLocalManager{
		db:            db,
		serializer:    s,
		badgerMapping: &sync.Map{},
		logger:        logger,
	}

	for i := 10; i <= 30; i++ {
		dnInfo := &management_models.DBInfo{
			Name:         fmt.Sprintf("badger-db-test-%d", i),
			Path:         fmt.Sprintf("test/path-%v", i),
			CreatedAt:    time.Now(),
			LastOpenedAt: time.Now(),
		}

		err = m.CreateOpenStoreAndLoadIntoMemory(dnInfo)
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

			logger.Debugf(
				"badger instance",
				go_logger.Field{
					"name": key,
					"info": value,
				},
			)

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
	logger.Debugf(
		"batch counters",
		go_logger.Field{
			"batch_counter": m.db.MaxBatchCount(),
			"batch_size":    m.db.MaxBatchSize(),
			"size":          fmt.Sprintf("%v - %v", s1, s2),
		},
	)

	m.ShutdownStores()
	m.Shutdown()

	m.badgerMapping.Range(
		func(key, value any) bool {
			// Should not be displayed
			logger.Warningf("remaining ->", map[string]interface{}{
				"key":   key,
				"value": value,
			})

			return true
		},
	)

	logger.Infof("cleaned up all successfully")
}

func Test_Integration_ListStoreInfoFromMemoryOrDisk(t *testing.T) {
	t.Run(
		"list with pagination",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger db instance")
			db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
			if err != nil {
				logger.Errorf(
					"failed to open badger local manager",
					go_logger.Mapper("err", err.Error()),
				)
			}

			s := go_serializer.NewJsonSerializer()

			logger.Infof("starting badger instances")
			m := &BadgerLocalManager{
				db:            db,
				serializer:    s,
				badgerMapping: &sync.Map{},
				logger:        logger,
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

					logger.Debugf(
						"badger instance",
						go_logger.Field{
							"name": key,
							"info": value,
						},
					)
				}

				return nil
			})
			require.NoError(t, err)

			logger.Debugf("###########################################################################################")

			list, err := m.ListStoreInfoFromMemoryOrDisk(ctx, 7, 4)
			require.NoError(t, err)

			logger.Debugf(
				"paginated list",
				go_logger.Field{
					"list": list,
				},
			)

			m.ShutdownStores()
			m.Shutdown()

			m.badgerMapping.Range(
				func(key, value any) bool {
					// Should not be displayed
					logger.Warningf("remaining ->", map[string]interface{}{
						"key":   key,
						"value": value,
					})

					return true
				},
			)

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"list with no pagination",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger db instance")
			db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
			if err != nil {
				logger.Errorf(
					"failed to open badger local manager",
					go_logger.Mapper("err", err.Error()),
				)
			}

			s := go_serializer.NewJsonSerializer()

			logger.Infof("starting badger instances")
			m := &BadgerLocalManager{
				db:            db,
				serializer:    s,
				badgerMapping: &sync.Map{},
				logger:        logger,
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

					logger.Debugf(
						"badger instance",
						go_logger.Field{
							"name": key,
							"info": value,
						},
					)
				}

				return nil
			})
			require.NoError(t, err)

			logger.Debugf("###########################################################################################")

			list, err := m.ListStoreInfoFromMemoryOrDisk(ctx, 0, 0)
			require.NoError(t, err)

			logger.Debugf(

				"not paginated list - list all",
				go_logger.Field{
					"list": list,
				},
			)

			m.ShutdownStores()
			m.Shutdown()

			m.badgerMapping.Range(
				func(key, value any) bool {
					// Should not be displayed
					logger.Warningf("remaining ->", map[string]interface{}{
						"key":   key,
						"value": value,
					})

					return true
				},
			)

			logger.Infof("cleaned up all successfully")
		},
	)
}

func Test_Integration_ListStoreMemoryInfoFromMemoryOrDisk(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	logger := go_logger.FromCtx(ctx)

	logger.Infof("opening badger db instance")
	db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
	if err != nil {
		logger.Errorf(
			"failed to open badger local manager",
			go_logger.Mapper("err", err.Error()),
		)
	}

	s := go_serializer.NewJsonSerializer()

	logger.Infof("starting badger instances")
	m := &BadgerLocalManager{
		db:            db,
		serializer:    s,
		badgerMapping: &sync.Map{},
		logger:        logger,
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

			logger.Debugf(
				"badger instance",
				go_logger.Field{
					"name": key,
					"info": value,
				},
			)
		}

		return nil
	})
	require.NoError(t, err)

	logger.Debugf("###################################################################################################")

	list, err := m.ListStoreMemoryInfoFromMemoryOrDisk(ctx, 7, 4)
	require.NoError(t, err)

	logger.Debugf(

		"paginated list",
		go_logger.Field{
			"list": list,
		},
	)

	m.ShutdownStores()
	m.Shutdown()

	m.badgerMapping.Range(
		func(key, value any) bool {
			// Should not be displayed
			logger.Warningf("remaining ->", map[string]interface{}{
				"key":   key,
				"value": value,
			})

			return true
		},
	)

	logger.Infof("cleaned up all successfully")
}
