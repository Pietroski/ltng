package indexed_manager

import (
	"context"
	"testing"
	"time"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"
	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
)

func Test_Integration_CreateIndexedStore(t *testing.T) {
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
	m := manager.NewBadgerLocalManager(db, s, logger)
	idxM := &BadgerLocalIndexerManager{
		serializer: s,
		db:         db,
		manager:    m,
	}
	err = m.Start()
	if err != nil {
		logger.Errorf(
			"failed to start badger instances",
			go_logger.Mapper("err", err.Error()),
		)
	}

	dbInfo := &management_models.DBInfo{
		Name:         "badger-db-test-1",
		Path:         "test/path-1",
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Now(),
	}

	err = idxM.CreateIndexedStore(ctx, dbInfo)
	require.NoError(t, err)

	dbMemoryInfo, err := idxM.manager.GetDBMemoryInfo(ctx, dbInfo.Name)
	require.NoError(t, err)
	t.Log(dbMemoryInfo)
	indexedDBInfoName := dbInfo.Name + IndexerSuffixName
	indexedDBMemoryInfo, err := idxM.manager.GetDBMemoryInfo(ctx, indexedDBInfoName)
	require.NoError(t, err)
	t.Log(indexedDBMemoryInfo)

	m.ShutdownStores()
	m.Shutdown()

	logger.Infof("cleaned up all successfully")
}

func Test_Integration_DeleteIndexedStore(t *testing.T) {
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
	m := manager.NewBadgerLocalManager(db, s, logger)
	idxM := &BadgerLocalIndexerManager{
		serializer: s,
		db:         db,
		manager:    m,
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

	err = idxM.CreateIndexedStore(ctx, dbInfo)
	require.NoError(t, err)

	dbMemoryInfo, err := idxM.manager.GetDBMemoryInfo(ctx, dbInfo.Name)
	require.NoError(t, err)
	t.Log(dbMemoryInfo)
	indexedDBInfoName := dbInfo.Name + IndexerSuffixName
	indexedDBMemoryInfo, err := idxM.manager.GetDBMemoryInfo(ctx, indexedDBInfoName)
	require.NoError(t, err)
	t.Log(indexedDBMemoryInfo)

	err = idxM.DeleteIndexedStore(ctx, dbInfo.Name)
	require.NoError(t, err)

	indexedDBMemoryInfo, err = idxM.manager.GetDBMemoryInfo(ctx, indexedDBInfoName)
	require.Error(t, err)
	t.Log("empty indexedDBMemoryInfo ->", indexedDBMemoryInfo)

	m.ShutdownStores()
	m.Shutdown()

	logger.Infof("cleaned up all successfully")
}

func Test_Integration_GetDBAndIndexedDBInfoStore(t *testing.T) {
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
	m := manager.NewBadgerLocalManager(db, s, logger)
	idxM := NewBadgerLocalIndexerManager(db, m, s)
	err = m.Start()
	if err != nil {
		logger.Errorf(
			"failed to start badger instances",
			go_logger.Mapper("err", err.Error()),
		)
	}

	payload := &management_models.DBInfo{
		Name:         "badger-db-test-3",
		Path:         "test/path-3",
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Now(),
	}

	err = idxM.CreateIndexedStore(ctx, payload)
	require.NoError(t, err)

	dbInfo, err := idxM.GetDBInfo(ctx, payload.Name)
	require.NoError(t, err)
	t.Log(dbInfo)
	//indexedDBInfoName := dbInfo.Name + IndexerSuffixName
	indexedDBInfo, err := idxM.GetIndexedDBInfo(ctx, payload.Name)
	require.NoError(t, err)
	t.Log(indexedDBInfo)

	dbInfoCheck, indexedDBInfoCheck, err :=
		idxM.GetDBAndIndexedDBInfo(ctx, dbInfo.Name)
	require.NoError(t, err)
	require.Equal(t, dbInfo, dbInfoCheck)
	require.Equal(t, indexedDBInfo, indexedDBInfoCheck)

	m.ShutdownStores()
	m.Shutdown()

	logger.Infof("cleaned up all successfully")
}

func Test_Integration_GetDBAndIndexedDBMemoryInfoStore(t *testing.T) {
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
	m := manager.NewBadgerLocalManager(db, s, logger)
	idxM := NewBadgerLocalIndexerManager(db, m, s)
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

	err = idxM.CreateIndexedStore(ctx, dbInfo)
	require.NoError(t, err)

	dbMemoryInfo, err := idxM.GetDBMemoryInfo(ctx, dbInfo.Name)
	require.NoError(t, err)
	t.Log(dbMemoryInfo)
	//indexedDBInfoName := dbInfo.Name + IndexerSuffixName
	indexedDBMemoryInfo, err := idxM.GetIndexedDBMemoryInfo(ctx, dbInfo.Name)
	require.NoError(t, err)
	t.Log(indexedDBMemoryInfo)

	dbMemoryInfoCheck, indexedDBMemoryInfoCheck, err :=
		idxM.GetDBAndIndexedDBMemoryInfo(ctx, dbInfo.Name)
	require.NoError(t, err)
	require.Equal(t, dbMemoryInfo, dbMemoryInfoCheck)
	require.Equal(t, indexedDBMemoryInfo, indexedDBMemoryInfoCheck)

	m.ShutdownStores()
	m.Shutdown()

	logger.Infof("cleaned up all successfully")
}

func Test_Integration_ListDBAndIndexedDBInfoStore(t *testing.T) {
	t.Run(
		"paginated",
		func(t *testing.T) {
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
			m := manager.NewBadgerLocalManager(db, s, logger)
			idxM := NewBadgerLocalIndexerManager(db, m, s)
			err = m.Start()
			if err != nil {
				logger.Errorf(
					"failed to start badger instances",
					go_logger.Mapper("err", err.Error()),
				)
			}

			payload := &management_models.DBInfo{
				Name:         "badger-db-test-4",
				Path:         "test/path-4",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}

			size, page := 7, 1

			err = idxM.CreateIndexedStore(ctx, payload)
			require.NoError(t, err)

			dbInfo, err := idxM.ListStoreInfo(ctx, size, page)
			require.NoError(t, err)
			t.Log(dbInfo)
			//indexedDBInfoName := dbInfo.Name + IndexerSuffixName
			indexedDBInfo, err := idxM.ListIndexedStoreInfo(ctx, size, page)
			require.NoError(t, err)
			t.Log("indexed list ->", indexedDBInfo)

			dbInfoCheck, indexedDBInfoCheck, err :=
				idxM.ListStoreAndIndexedStoreInfo(ctx, size, page)
			require.NoError(t, err)
			require.Equal(t, dbInfo, dbInfoCheck)
			require.Equal(t, indexedDBInfo, indexedDBInfoCheck)

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"not paginated",
		func(t *testing.T) {
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
			m := manager.NewBadgerLocalManager(db, s, logger)
			idxM := NewBadgerLocalIndexerManager(db, m, s)
			err = m.Start()
			if err != nil {
				logger.Errorf(
					"failed to start badger instances",
					go_logger.Mapper("err", err.Error()),
				)
			}

			payload := &management_models.DBInfo{
				Name:         "badger-db-test-4",
				Path:         "test/path-4",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}

			size, page := 0, 0

			err = idxM.CreateIndexedStore(ctx, payload)
			require.NoError(t, err)

			dbInfo, err := idxM.ListStoreInfo(ctx, size, page)
			require.NoError(t, err)
			t.Log(dbInfo)
			//indexedDBInfoName := dbInfo.Name + IndexerSuffixName
			indexedDBInfo, err := idxM.ListIndexedStoreInfo(ctx, size, page)
			require.NoError(t, err)
			t.Log(indexedDBInfo)

			dbInfoCheck, indexedDBInfoCheck, err :=
				idxM.ListStoreAndIndexedStoreInfo(ctx, size, page)
			require.NoError(t, err)
			require.Equal(t, dbInfo, dbInfoCheck)
			require.Equal(t, indexedDBInfo, indexedDBInfoCheck)

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)
}

func Test_Integration_ListDBAndIndexedDBMemoryInfoStore(t *testing.T) {
	t.Run(
		"paginated",
		func(t *testing.T) {
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
			m := manager.NewBadgerLocalManager(db, s, logger)
			idxM := NewBadgerLocalIndexerManager(db, m, s)
			err = m.Start()
			if err != nil {
				logger.Errorf(
					"failed to start badger instances",
					go_logger.Mapper("err", err.Error()),
				)
			}

			payload := &management_models.DBInfo{
				Name:         "badger-db-test-4",
				Path:         "test/path-4",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}

			err = idxM.CreateIndexedStore(ctx, payload)
			require.NoError(t, err)

			size, page := 7, 1

			dbInfo, err := idxM.ListStoreMemoryInfo(ctx, size, page)
			require.NoError(t, err)
			t.Log(dbInfo)
			//indexedDBInfoName := dbInfo.Name + IndexerSuffixName
			indexedDBInfo, err := idxM.ListIndexedStoreMemoryInfo(ctx, size, page)
			require.NoError(t, err)
			t.Log("indexedDBInfo list ->", indexedDBInfo)

			dbInfoCheck, indexedDBInfoCheck, err :=
				idxM.ListStoreAndIndexedStoreMemoryInfo(ctx, size, page)
			require.NoError(t, err)
			require.Equal(t, dbInfo, dbInfoCheck)
			require.Equal(t, indexedDBInfo, indexedDBInfoCheck)

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"not paginated",
		func(t *testing.T) {
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
			m := manager.NewBadgerLocalManager(db, s, logger)
			idxM := NewBadgerLocalIndexerManager(db, m, s)
			err = m.Start()
			if err != nil {
				logger.Errorf(
					"failed to start badger instances",
					go_logger.Mapper("err", err.Error()),
				)
			}

			payload := &management_models.DBInfo{
				Name:         "badger-db-test-4",
				Path:         "test/path-4",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}

			size, page := 0, 0

			err = idxM.CreateIndexedStore(ctx, payload)
			require.NoError(t, err)

			dbInfo, err := idxM.ListStoreMemoryInfo(ctx, size, page)
			require.NoError(t, err)
			t.Log(dbInfo)
			//indexedDBInfoName := dbInfo.Name + IndexerSuffixName
			indexedDBInfo, err := idxM.ListIndexedStoreMemoryInfo(ctx, size, page)
			require.NoError(t, err)
			t.Log(indexedDBInfo)

			dbInfoCheck, indexedDBInfoCheck, err :=
				idxM.ListStoreAndIndexedStoreMemoryInfo(ctx, size, page)
			require.NoError(t, err)
			require.Equal(t, dbInfo, dbInfoCheck)
			require.Equal(t, indexedDBInfo, indexedDBInfoCheck)

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)
}
