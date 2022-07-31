package indexed_operations

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager"
	indexed_manager "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager/indexed"
	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/transactions/operations"
	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation"
	indexed_operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation/indexed"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
	go_tracer "gitlab.com/pietroski-software-company/tools/tracer/go-tracer/v2/pkg/tools/tracer"
)

func Test_Shallow_Integration_CreateIndexed(t *testing.T) {
	t.Run(
		"create happy path call",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			ctx, err := go_tracer.NewCtxTracer().Trace(ctx)
			require.NoError(t, err)
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			if err != nil {
				logger.Errorf(
					"failed to open badger local manager",
					go_logger.Mapper("err", err.Error()),
				)
				require.NoError(t, err)
			}

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			idxM := indexed_manager.NewBadgerLocalIndexerManager(db, m, serializer)
			op := operations.NewBadgerOperator(m, serializer)
			idxOps := NewBadgerIndexedOperator(m, idxM, op, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			if err := idxM.CreateIndexedStore(ctx, dbInfoOpTest); err != nil {
				logger.Errorf(
					"error on creating operation test integration database",
					go_logger.Field{
						"error":                  err.Error(),
						"db_info_operation_test": dbInfoOpTest,
					},
				)
				require.NoError(t, err)
			}
			logger.Debugf("store created")

			dbMemoryInfo, _, err := idxM.GetDBAndIndexedDBMemoryInfo(ctx, dbInfoOpTest.Name)
			if err != nil {
				logger.Errorf(
					"error on retrieving memory info from given db name",
					go_logger.Field{
						"error":                  err.Error(),
						"db_info_operation_test": dbInfoOpTest,
					},
				)
				require.NoError(t, err)
			}

			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			indexStoreKey := []byte("index-key-test-to-catch")
			item := &operation_models.OpItem{
				Key:   storeKey,
				Value: storeValue,
			}
			indexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  storeKey,
				IndexKeys:  [][]byte{indexStoreKey},
			}
			if err := idxOps.Operate(dbMemoryInfo).CreateIndexed(ctx, item, indexing); err != nil {
				logger.Errorf(
					"error on storing key value pair on given storage",
					go_logger.Field{
						"error": err.Error(),
						"key":   storeKey,
						"value": storeValue,
					},
				)
				require.NoError(t, err)
			}

			searchItem := &operation_models.OpItem{}
			searchIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.One,
				},
			}
			retrievedValue, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			if err != nil {
				logger.Errorf(
					"error on storing key value pair on given storage",
					go_logger.Field{
						"error": err.Error(),
						"key":   storeKey,
						"value": storeValue,
					},
				)
				require.NoError(t, err)
			}

			t.Log("retrieved value ->", string(retrievedValue))
			require.Equal(t, retrievedValue, storeValue)

			searchItem = &operation_models.OpItem{
				Key: storeKey,
			}
			searchIndexing = &indexed_operation_models.IdxOpsIndex{
				ShallIndex: false,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.One,
				},
			}
			retrievedValueNoIdx, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			if err != nil {
				logger.Errorf(
					"error on storing key value pair on given storage",
					go_logger.Field{
						"error": err.Error(),
						"key":   storeKey,
						"value": storeValue,
					},
				)
				require.NoError(t, err)
			}

			t.Log("retrieved value no idx ->", string(retrievedValueNoIdx))
			require.Equal(t, retrievedValueNoIdx, storeValue)

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"update happy path call",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			ctx, err := go_tracer.NewCtxTracer().Trace(ctx)
			require.NoError(t, err)
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			if err != nil {
				logger.Errorf(
					"failed to open badger local manager",
					go_logger.Mapper("err", err.Error()),
				)
				require.NoError(t, err)
			}

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			idxM := indexed_manager.NewBadgerLocalIndexerManager(db, m, serializer)
			op := operations.NewBadgerOperator(m, serializer)
			idxOps := NewBadgerIndexedOperator(m, idxM, op, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			if err := idxM.CreateIndexedStore(ctx, dbInfoOpTest); err != nil {
				logger.Errorf(
					"error on creating operation test integration database",
					go_logger.Field{
						"error":                  err.Error(),
						"db_info_operation_test": dbInfoOpTest,
					},
				)
				require.NoError(t, err)
			}
			logger.Debugf("store created")

			dbMemoryInfo, _, err := idxM.GetDBAndIndexedDBMemoryInfo(ctx, dbInfoOpTest.Name)
			if err != nil {
				logger.Errorf(
					"error on retrieving memory info from given db name",
					go_logger.Field{
						"error":                  err.Error(),
						"db_info_operation_test": dbInfoOpTest,
					},
				)
				require.NoError(t, err)
			}

			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			indexStoreKey := []byte("index-key-test-to-catch")
			item := &operation_models.OpItem{
				Key:   storeKey,
				Value: storeValue,
			}
			indexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  storeKey,
				IndexKeys:  [][]byte{indexStoreKey},
			}
			if err := idxOps.Operate(dbMemoryInfo).UpdateIndexed(ctx, item, indexing); err != nil {
				logger.Errorf(
					"error on storing key value pair on given storage",
					go_logger.Field{
						"error": err.Error(),
						"key":   storeKey,
						"value": storeValue,
					},
				)
				require.NoError(t, err)
			}

			searchItem := &operation_models.OpItem{}
			searchIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.One,
				},
			}
			retrievedValue, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			if err != nil {
				logger.Errorf(
					"error on storing key value pair on given storage",
					go_logger.Field{
						"error": err.Error(),
						"key":   storeKey,
						"value": storeValue,
					},
				)
				require.NoError(t, err)
			}

			t.Log("retrieved value ->", string(retrievedValue))
			require.Equal(t, retrievedValue, storeValue)

			searchItem = &operation_models.OpItem{
				Key: storeKey,
			}
			searchIndexing = &indexed_operation_models.IdxOpsIndex{
				ShallIndex: false,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.One,
				},
			}
			retrievedValueNoIdx, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			if err != nil {
				logger.Errorf(
					"error on storing key value pair on given storage",
					go_logger.Field{
						"error": err.Error(),
						"key":   storeKey,
						"value": storeValue,
					},
				)
				require.NoError(t, err)
			}

			t.Log("retrieved value no idx ->", string(retrievedValueNoIdx))
			require.Equal(t, retrievedValueNoIdx, storeValue)

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"create happy path call",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			ctx, err := go_tracer.NewCtxTracer().Trace(ctx)
			require.NoError(t, err)
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			require.NoError(t, err)

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			idxM := indexed_manager.NewBadgerLocalIndexerManager(db, m, serializer)
			op := operations.NewBadgerOperator(m, serializer)
			idxOps := NewBadgerIndexedOperator(m, idxM, op, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = idxM.CreateIndexedStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, _, err := idxM.GetDBAndIndexedDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			indexStoreKey := []byte("index-key-test-to-catch")
			item := &operation_models.OpItem{
				Key:   storeKey,
				Value: storeValue,
			}
			indexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: false,
			}
			err = idxOps.Operate(dbMemoryInfo).CreateIndexed(ctx, item, indexing)
			require.NoError(t, err)

			searchItem := &operation_models.OpItem{
				Key: storeKey,
			}
			searchIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: false,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.One,
				},
			}
			retrievedValueNoIdx, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.NoError(t, err)
			t.Log("retrieved value no idx ->", string(retrievedValueNoIdx))
			require.Equal(t, retrievedValueNoIdx, storeValue)

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"update happy path call",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			ctx, err := go_tracer.NewCtxTracer().Trace(ctx)
			require.NoError(t, err)
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			require.NoError(t, err)

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			idxM := indexed_manager.NewBadgerLocalIndexerManager(db, m, serializer)
			op := operations.NewBadgerOperator(m, serializer)
			idxOps := NewBadgerIndexedOperator(m, idxM, op, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = idxM.CreateIndexedStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, _, err := idxM.GetDBAndIndexedDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			indexStoreKey := []byte("index-key-test-to-catch")
			item := &operation_models.OpItem{
				Key:   storeKey,
				Value: storeValue,
			}
			indexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: false,
			}
			err = idxOps.Operate(dbMemoryInfo).UpdateIndexed(ctx, item, indexing)
			require.NoError(t, err)

			searchItem := &operation_models.OpItem{
				Key: storeKey,
			}
			searchIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: false,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.One,
				},
			}
			retrievedValueNoIdx, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.NoError(t, err)
			t.Log("retrieved value no idx ->", string(retrievedValueNoIdx))
			require.Equal(t, retrievedValueNoIdx, storeValue)

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)
}

func Test_Shallow_Integration_Create_Load_Delete_Load_Indexed(t *testing.T) {
	t.Run(
		"happy path flow - deleting on cascade",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			ctx, err := go_tracer.NewCtxTracer().Trace(ctx)
			require.NoError(t, err)
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			require.NoError(t, err)

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			idxM := indexed_manager.NewBadgerLocalIndexerManager(db, m, serializer)
			op := operations.NewBadgerOperator(m, serializer)
			idxOps := NewBadgerIndexedOperator(m, idxM, op, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test-2",
				Path:         "operations-db-integration-test-2",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = idxM.CreateIndexedStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, _, err := idxM.GetDBAndIndexedDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			logger.Debugf("creating entry")
			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			indexStoreKey := []byte("index-key-test-to-catch")
			item := &operation_models.OpItem{
				Key:   storeKey,
				Value: storeValue,
			}
			indexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  storeKey,
				IndexKeys:  [][]byte{indexStoreKey},
			}
			err = idxOps.Operate(dbMemoryInfo).UpdateIndexed(ctx, item, indexing)
			require.NoError(t, err)
			logger.Debugf("entry created")

			logger.Debugf("loading entry")
			searchItem := &operation_models.OpItem{}
			searchIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.One,
				},
			}
			retrievedValue, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.NoError(t, err)
			logger.Debugf("loaded entry")

			t.Log("retrieved value ->", string(retrievedValue))
			require.Equal(t, retrievedValue, storeValue)

			logger.Debugf("loading entry")
			searchItem = &operation_models.OpItem{
				Key: storeKey,
			}
			searchIndexing = &indexed_operation_models.IdxOpsIndex{
				ShallIndex: false,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.One,
				},
			}
			retrievedValueNoIdx, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.NoError(t, err)
			t.Log("retrieved value no idx ->", string(retrievedValueNoIdx))
			require.Equal(t, retrievedValueNoIdx, storeValue)
			logger.Debugf("loaded entry")

			logger.Debugf("deleting entry - cascade mode")
			deleteItem := &operation_models.OpItem{
				Key: storeKey,
			}
			deleteIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				//ParentKey:  nil,
				//IndexKeys:  [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexDeletionBehaviour: indexed_operation_models.Cascade,
				},
			}
			err = idxOps.Operate(dbMemoryInfo).DeleteIndexed(ctx, deleteItem, deleteIndexing)
			require.NoError(t, err)
			logger.Debugf("deleted entry")

			logger.Debugf("loading entry")
			retrievedValueNoIdx, err = idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.Error(t, err)
			searchIndexing = &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.One,
				},
			}
			logger.Debugf("loaded entry")
			logger.Debugf("loading entry")
			retrievedValue, err = idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.Error(t, err)
			logger.Debugf("loaded entry")

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"happy path flow - deleting index only",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			ctx, err := go_tracer.NewCtxTracer().Trace(ctx)
			require.NoError(t, err)
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			require.NoError(t, err)

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			idxM := indexed_manager.NewBadgerLocalIndexerManager(db, m, serializer)
			op := operations.NewBadgerOperator(m, serializer)
			idxOps := NewBadgerIndexedOperator(m, idxM, op, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test-2",
				Path:         "operations-db-integration-test-2",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = idxM.CreateIndexedStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, _, err := idxM.GetDBAndIndexedDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			logger.Debugf("creating entry")
			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			indexStoreKey := []byte("index-key-test-to-catch")
			item := &operation_models.OpItem{
				Key:   storeKey,
				Value: storeValue,
			}
			indexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  storeKey,
				IndexKeys:  [][]byte{indexStoreKey},
			}
			err = idxOps.Operate(dbMemoryInfo).UpdateIndexed(ctx, item, indexing)
			require.NoError(t, err)
			logger.Debugf("entry created")

			logger.Debugf("loading entry")
			searchItem := &operation_models.OpItem{}
			searchIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.One,
				},
			}
			retrievedValue, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.NoError(t, err)
			logger.Debugf("loaded entry")

			t.Log("retrieved value ->", string(retrievedValue))
			require.Equal(t, retrievedValue, storeValue)

			logger.Debugf("loading entry")
			searchItem = &operation_models.OpItem{
				Key: storeKey,
			}
			searchIndexing = &indexed_operation_models.IdxOpsIndex{
				ShallIndex: false,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.One,
				},
			}
			retrievedValueNoIdx, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.NoError(t, err)
			t.Log("retrieved value no idx ->", string(retrievedValueNoIdx))
			require.Equal(t, retrievedValueNoIdx, storeValue)
			logger.Debugf("loaded entry")

			logger.Debugf("deleting entry - index only mode")
			deleteItem := &operation_models.OpItem{}
			deleteIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				//ParentKey:  nil,
				IndexKeys: [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexDeletionBehaviour: indexed_operation_models.IndexOnly,
				},
			}
			err = idxOps.Operate(dbMemoryInfo).DeleteIndexed(ctx, deleteItem, deleteIndexing)
			require.NoError(t, err)
			logger.Debugf("deleted entry")

			logger.Debugf("loading entry")
			searchIndexing = &indexed_operation_models.IdxOpsIndex{
				ShallIndex: false,
			}
			retrievedValueNoIdx, err = idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.NoError(t, err)
			require.Equal(t, searchItem.Key, storeKey)
			searchIndexing = &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.One,
				},
			}
			logger.Debugf("loaded entry")
			logger.Debugf("loading entry")
			retrievedValue, err = idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, nil, searchIndexing)
			logger.Infof("loaded entry ->", go_logger.Field{"retrievedValue": retrievedValue})
			require.Error(t, err)
			logger.Debugf("loaded entry")

			logger.Debugf("loading entry list")
			pagination := &management_models.PaginationRequest{
				PageID:   0,
				PageSize: 0,
			}
			retrievedValueList, err := idxOps.Operate(dbMemoryInfo).ListIndexed(ctx, searchIndexing, pagination)
			logger.Infof("loaded entry ->", go_logger.Field{"retrievedValueList": retrievedValueList})
			require.NoError(t, err)
			require.Nil(t, retrievedValueList)
			require.Empty(t, retrievedValueList)
			logger.Debugf("loaded entry list")

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"happy path flow - deleting none",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			ctx, err := go_tracer.NewCtxTracer().Trace(ctx)
			require.NoError(t, err)
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			require.NoError(t, err)

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			idxM := indexed_manager.NewBadgerLocalIndexerManager(db, m, serializer)
			op := operations.NewBadgerOperator(m, serializer)
			idxOps := NewBadgerIndexedOperator(m, idxM, op, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test-2",
				Path:         "operations-db-integration-test-2",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = idxM.CreateIndexedStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, _, err := idxM.GetDBAndIndexedDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			logger.Debugf("creating entry")
			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			indexStoreKey := []byte("index-key-test-to-catch")
			item := &operation_models.OpItem{
				Key:   storeKey,
				Value: storeValue,
			}
			indexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  storeKey,
				IndexKeys:  [][]byte{indexStoreKey},
			}
			err = idxOps.Operate(dbMemoryInfo).UpdateIndexed(ctx, item, indexing)
			require.NoError(t, err)
			logger.Debugf("entry created")

			logger.Debugf("loading entry")
			searchItem := &operation_models.OpItem{}
			searchIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.One,
				},
			}
			retrievedValue, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.NoError(t, err)
			logger.Debugf("loaded entry")

			t.Log("retrieved value ->", string(retrievedValue))
			require.Equal(t, retrievedValue, storeValue)

			logger.Debugf("loading entry")
			searchItem = &operation_models.OpItem{
				Key: storeKey,
			}
			searchIndexing = &indexed_operation_models.IdxOpsIndex{
				ShallIndex: false,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.One,
				},
			}
			retrievedValueNoIdx, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.NoError(t, err)
			t.Log("retrieved value no idx ->", string(retrievedValueNoIdx))
			require.Equal(t, retrievedValueNoIdx, storeValue)
			logger.Debugf("loaded entry")

			logger.Debugf("deleting entry - index only mode")
			deleteItem := &operation_models.OpItem{
				Key: storeKey,
			}
			deleteIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				//ParentKey:  nil,
				IndexKeys: [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexDeletionBehaviour: indexed_operation_models.None,
				},
			}
			err = idxOps.Operate(dbMemoryInfo).DeleteIndexed(ctx, deleteItem, deleteIndexing)
			require.NoError(t, err)
			logger.Debugf("deleted entry")

			logger.Debugf("loading entry")
			searchIndexing = &indexed_operation_models.IdxOpsIndex{
				ShallIndex: false,
			}
			retrievedValueNoIdx, err = idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.Error(t, err)
			require.Equal(t, searchItem.Key, storeKey)
			searchIndexing = &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.One,
				},
			}
			logger.Debugf("loaded entry")
			logger.Debugf("loading entry")
			retrievedValue, err = idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, nil, searchIndexing)
			logger.Infof("loaded entry ->", go_logger.Field{"retrievedValue": retrievedValue})
			require.Error(t, err)
			logger.Debugf("loaded entry")

			logger.Debugf("loading entry list")
			pagination := &management_models.PaginationRequest{
				PageID:   0,
				PageSize: 0,
			}
			retrievedValueList, err := idxOps.Operate(dbMemoryInfo).ListIndexed(ctx, searchIndexing, pagination)
			logger.Infof("loaded entry ->", go_logger.Field{"retrievedValueList": retrievedValueList})
			require.NoError(t, err)
			require.Nil(t, retrievedValueList)
			require.Empty(t, retrievedValueList)
			logger.Debugf("loaded entry list")

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"happy path flow - deleting no index",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			ctx, err := go_tracer.NewCtxTracer().Trace(ctx)
			require.NoError(t, err)
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			require.NoError(t, err)

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			idxM := indexed_manager.NewBadgerLocalIndexerManager(db, m, serializer)
			op := operations.NewBadgerOperator(m, serializer)
			idxOps := NewBadgerIndexedOperator(m, idxM, op, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test-2",
				Path:         "operations-db-integration-test-2",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = idxM.CreateIndexedStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, _, err := idxM.GetDBAndIndexedDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			logger.Debugf("creating entry")
			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			indexStoreKey := []byte("index-key-test-to-catch")
			item := &operation_models.OpItem{
				Key:   storeKey,
				Value: storeValue,
			}
			indexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: false,
				ParentKey:  storeKey,
				IndexKeys:  [][]byte{indexStoreKey},
			}
			err = idxOps.Operate(dbMemoryInfo).UpdateIndexed(ctx, item, indexing)
			require.NoError(t, err)
			logger.Debugf("entry created")

			logger.Debugf("loading entry")
			searchItem := &operation_models.OpItem{
				Key: storeKey,
			}
			searchIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: false,
			}
			retrievedValue, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.NoError(t, err)
			logger.Debugf("loaded entry")
			t.Log("retrieved value ->", string(retrievedValue))
			require.Equal(t, retrievedValue, storeValue)

			logger.Debugf("deleting entry - index only mode")
			deleteItem := &operation_models.OpItem{
				Key: storeKey,
			}
			deleteIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: false,
			}
			err = idxOps.Operate(dbMemoryInfo).DeleteIndexed(ctx, deleteItem, deleteIndexing)
			require.NoError(t, err)
			logger.Debugf("deleted entry")

			logger.Debugf("loading entry")
			searchItem = &operation_models.OpItem{
				Key: storeKey,
			}
			searchIndexing = &indexed_operation_models.IdxOpsIndex{
				ShallIndex: false,
			}
			retrievedValue, err = idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.Error(t, err)
			logger.Debugf("loaded entry")
			t.Log("retrieved value ->", string(retrievedValue))
			require.Equal(t, retrievedValue, []byte(nil))

			logger.Debugf("loading entry list")
			pagination := &management_models.PaginationRequest{
				PageID:   0,
				PageSize: 0,
			}
			retrievedValueList, err := idxOps.Operate(dbMemoryInfo).ListIndexed(ctx, searchIndexing, pagination)
			logger.Infof("loaded entry ->", go_logger.Field{"retrievedValueList": retrievedValueList})
			require.NoError(t, err)
			require.Nil(t, retrievedValueList)
			require.Empty(t, retrievedValueList)
			logger.Debugf("loaded entry list")

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)
}

func Test_Shallow_Integration_Create_Load(t *testing.T) {
	t.Run(
		"load AND computational",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			ctx, err := go_tracer.NewCtxTracer().Trace(ctx)
			require.NoError(t, err)
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			require.NoError(t, err)

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			idxM := indexed_manager.NewBadgerLocalIndexerManager(db, m, serializer)
			op := operations.NewBadgerOperator(m, serializer)
			idxOps := NewBadgerIndexedOperator(m, idxM, op, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test-4",
				Path:         "operations-db-integration-test-4",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = idxM.CreateIndexedStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, _, err := idxM.GetDBAndIndexedDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			logger.Debugf("creating entry")
			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			indexStoreKey1 := []byte("index-key-test-to-catch-1")
			indexStoreKey2 := []byte("index-key-test-to-catch-2")
			item := &operation_models.OpItem{
				Key:   storeKey,
				Value: storeValue,
			}
			indexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  storeKey,
				IndexKeys:  [][]byte{indexStoreKey1, indexStoreKey2},
			}
			err = idxOps.Operate(dbMemoryInfo).UpdateIndexed(ctx, item, indexing)
			require.NoError(t, err)
			logger.Debugf("entry created")

			logger.Debugf("loading entry")
			searchItem := &operation_models.OpItem{}
			searchIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey1, indexStoreKey2},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.AndComputational,
				},
			}
			retrievedValue, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.NoError(t, err)
			logger.Debugf("loaded entry")

			t.Log("retrieved value ->", string(retrievedValue))
			require.Equal(t, retrievedValue, storeValue)

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"load AND computational - fails",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			ctx, err := go_tracer.NewCtxTracer().Trace(ctx)
			require.NoError(t, err)
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			require.NoError(t, err)

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			idxM := indexed_manager.NewBadgerLocalIndexerManager(db, m, serializer)
			op := operations.NewBadgerOperator(m, serializer)
			idxOps := NewBadgerIndexedOperator(m, idxM, op, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test-4",
				Path:         "operations-db-integration-test-4",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = idxM.CreateIndexedStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, _, err := idxM.GetDBAndIndexedDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			logger.Debugf("creating entry")
			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			indexStoreKey1 := []byte("index-key-test-to-catch-1")
			indexStoreKey2 := []byte("index-key-test-to-catch-2")
			item := &operation_models.OpItem{
				Key:   storeKey,
				Value: storeValue,
			}
			indexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  storeKey,
				IndexKeys:  [][]byte{indexStoreKey1, indexStoreKey2},
			}
			err = idxOps.Operate(dbMemoryInfo).UpdateIndexed(ctx, item, indexing)
			require.NoError(t, err)
			logger.Debugf("entry created")

			newStoreKey := []byte("new-key-test-to-store")
			newStoreValue := []byte("new value test to store")
			indexStoreKey3 := []byte("index-key-test-to-catch-3")
			newItem := &operation_models.OpItem{
				Key:   newStoreKey,
				Value: newStoreValue,
			}
			newIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  newStoreKey,
				IndexKeys:  [][]byte{indexStoreKey3},
			}
			err = idxOps.Operate(dbMemoryInfo).UpdateIndexed(ctx, newItem, newIndexing)
			require.NoError(t, err)
			logger.Debugf("new entry created")

			logger.Debugf("loading entry")
			searchItem := &operation_models.OpItem{}
			searchIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey1, indexStoreKey2, indexStoreKey3},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.AndComputational,
				},
			}
			retrievedValue, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.Error(t, err)
			logger.Debugf("loaded entry")

			t.Log("retrieved value ->", string(retrievedValue))
			require.Equal(t, retrievedValue, []byte(nil))

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"load OR computational",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			ctx, err := go_tracer.NewCtxTracer().Trace(ctx)
			require.NoError(t, err)
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			require.NoError(t, err)

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			idxM := indexed_manager.NewBadgerLocalIndexerManager(db, m, serializer)
			op := operations.NewBadgerOperator(m, serializer)
			idxOps := NewBadgerIndexedOperator(m, idxM, op, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test-4",
				Path:         "operations-db-integration-test-4",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = idxM.CreateIndexedStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, _, err := idxM.GetDBAndIndexedDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			logger.Debugf("creating entry")
			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			indexStoreKey1 := []byte("index-key-test-to-catch-1")
			indexStoreKey2 := []byte("index-key-test-to-catch-2")
			item := &operation_models.OpItem{
				Key:   storeKey,
				Value: storeValue,
			}
			indexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  storeKey,
				IndexKeys:  [][]byte{indexStoreKey1, indexStoreKey2},
			}
			err = idxOps.Operate(dbMemoryInfo).UpdateIndexed(ctx, item, indexing)
			require.NoError(t, err)
			logger.Debugf("entry created")

			logger.Debugf("loading entry")
			searchItem := &operation_models.OpItem{}
			searchIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey1, indexStoreKey2},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.OrComputational,
				},
			}
			retrievedValue, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.NoError(t, err)
			logger.Debugf("loaded entry")

			t.Log("retrieved value ->", string(retrievedValue))
			require.Equal(t, retrievedValue, storeValue)

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"load OR computational - fails",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			ctx, err := go_tracer.NewCtxTracer().Trace(ctx)
			require.NoError(t, err)
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			require.NoError(t, err)

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			idxM := indexed_manager.NewBadgerLocalIndexerManager(db, m, serializer)
			op := operations.NewBadgerOperator(m, serializer)
			idxOps := NewBadgerIndexedOperator(m, idxM, op, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test-4",
				Path:         "operations-db-integration-test-4",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = idxM.CreateIndexedStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, _, err := idxM.GetDBAndIndexedDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			logger.Debugf("creating entry")
			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			indexStoreKey1 := []byte("index-key-test-to-catch-1")
			indexStoreKey2 := []byte("index-key-test-to-catch-2")
			item := &operation_models.OpItem{
				Key:   storeKey,
				Value: storeValue,
			}
			indexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  storeKey,
				IndexKeys:  [][]byte{indexStoreKey1, indexStoreKey2},
			}
			err = idxOps.Operate(dbMemoryInfo).UpdateIndexed(ctx, item, indexing)
			require.NoError(t, err)
			logger.Debugf("entry created")

			newStoreKey := []byte("new-key-test-to-store")
			newStoreValue := []byte("new value test to store")
			indexStoreKey3 := []byte("index-key-test-to-catch-3")
			newItem := &operation_models.OpItem{
				Key:   newStoreKey,
				Value: newStoreValue,
			}
			newIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  newStoreKey,
				IndexKeys:  [][]byte{indexStoreKey3},
			}
			err = idxOps.Operate(dbMemoryInfo).UpdateIndexed(ctx, newItem, newIndexing)
			require.NoError(t, err)
			logger.Debugf("new entry created")

			logger.Debugf("loading entry")
			searchItem := &operation_models.OpItem{}
			searchIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey1, indexStoreKey2, indexStoreKey3},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.OrComputational,
				},
			}
			retrievedValue, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.Error(t, err)
			logger.Debugf("loaded entry")

			t.Log("retrieved value ->", string(retrievedValue))
			require.Equal(t, retrievedValue, []byte(nil))

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"load OR computational - no index",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			ctx, err := go_tracer.NewCtxTracer().Trace(ctx)
			require.NoError(t, err)
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			require.NoError(t, err)

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			idxM := indexed_manager.NewBadgerLocalIndexerManager(db, m, serializer)
			op := operations.NewBadgerOperator(m, serializer)
			idxOps := NewBadgerIndexedOperator(m, idxM, op, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test-3",
				Path:         "operations-db-integration-test-3",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = idxM.CreateIndexedStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, _, err := idxM.GetDBAndIndexedDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			logger.Debugf("creating entry")
			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			indexStoreKey1 := []byte("index-key-test-to-catch-1")
			indexStoreKey2 := []byte("index-key-test-to-catch-2")
			item := &operation_models.OpItem{
				Key:   storeKey,
				Value: storeValue,
			}
			indexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  storeKey,
				IndexKeys:  [][]byte{indexStoreKey1, indexStoreKey2},
			}
			err = idxOps.Operate(dbMemoryInfo).UpdateIndexed(ctx, item, indexing)
			require.NoError(t, err)
			logger.Debugf("entry created")

			indexStoreKey3 := []byte("index-key-test-to-catch-3")

			logger.Debugf("loading entry")
			searchItem := &operation_models.OpItem{}
			searchIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey1, indexStoreKey2, indexStoreKey3},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.OrComputational,
				},
			}
			retrievedValue, err := idxOps.Operate(dbMemoryInfo).LoadIndexed(ctx, searchItem, searchIndexing)
			require.NoError(t, err)
			logger.Debugf("loaded entry")

			t.Log("retrieved value ->", string(retrievedValue))
			require.Equal(t, retrievedValue, storeValue)

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)
}

func Test_Shallow_Integration_Create_List(t *testing.T) {
	t.Run(
		"list paginated",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			ctx, err := go_tracer.NewCtxTracer().Trace(ctx)
			require.NoError(t, err)
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			require.NoError(t, err)

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			idxM := indexed_manager.NewBadgerLocalIndexerManager(db, m, serializer)
			op := operations.NewBadgerOperator(m, serializer)
			idxOps := NewBadgerIndexedOperator(m, idxM, op, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test-5",
				Path:         "operations-db-integration-test-5",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = idxM.CreateIndexedStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, _, err := idxM.GetDBAndIndexedDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			logger.Debugf("creating entry")
			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			indexStoreKey1 := []byte("index-key-test-to-catch-1")
			indexStoreKey2 := []byte("index-key-test-to-catch-2")
			item := &operation_models.OpItem{
				Key:   storeKey,
				Value: storeValue,
			}
			indexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  storeKey,
				IndexKeys:  [][]byte{indexStoreKey1, indexStoreKey2},
			}
			err = idxOps.Operate(dbMemoryInfo).UpdateIndexed(ctx, item, indexing)
			require.NoError(t, err)
			logger.Debugf("entry created")

			logger.Debugf("loading entry")
			searchIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey1, indexStoreKey2},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.AndComputational,
				},
			}
			searchPagination := &management_models.PaginationRequest{
				PageID:   1,
				PageSize: 2,
			}
			retrievedValues, err := idxOps.Operate(dbMemoryInfo).ListIndexed(ctx, searchIndexing, searchPagination)
			require.NoError(t, err)
			logger.Debugf("loaded entry")

			t.Log("retrieved values ->", retrievedValues)
			for _, retrievedValue := range retrievedValues {
				t.Log("retrieved value ->", string(retrievedValue.Key), string(retrievedValue.Value))
			}
			//require.Equal(t, retrievedValue, storeValue)

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"list paginated",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			ctx, err := go_tracer.NewCtxTracer().Trace(ctx)
			require.NoError(t, err)
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			require.NoError(t, err)

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			idxM := indexed_manager.NewBadgerLocalIndexerManager(db, m, serializer)
			op := operations.NewBadgerOperator(m, serializer)
			idxOps := NewBadgerIndexedOperator(m, idxM, op, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test-5",
				Path:         "operations-db-integration-test-5",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = idxM.CreateIndexedStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, _, err := idxM.GetDBAndIndexedDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			logger.Debugf("creating entry")
			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			indexStoreKey1 := []byte("index-key-test-to-catch-1")
			indexStoreKey2 := []byte("index-key-test-to-catch-2")
			item := &operation_models.OpItem{
				Key:   storeKey,
				Value: storeValue,
			}
			indexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  storeKey,
				IndexKeys:  [][]byte{indexStoreKey1, indexStoreKey2},
			}
			err = idxOps.Operate(dbMemoryInfo).UpdateIndexed(ctx, item, indexing)
			require.NoError(t, err)
			logger.Debugf("entry created")

			logger.Debugf("loading entry")
			searchIndexing := &indexed_operation_models.IdxOpsIndex{
				ShallIndex: true,
				ParentKey:  nil,
				IndexKeys:  [][]byte{indexStoreKey1, indexStoreKey2},
				IndexProperties: indexed_operation_models.IndexProperties{
					IndexSearchPattern: indexed_operation_models.AndComputational,
				},
			}
			searchPagination := &management_models.PaginationRequest{
				PageID:   2,
				PageSize: 1,
			}
			retrievedValues, err := idxOps.Operate(dbMemoryInfo).ListIndexed(ctx, searchIndexing, searchPagination)
			require.NoError(t, err)
			logger.Debugf("loaded entry")

			t.Log("retrieved values ->", retrievedValues, len(retrievedValues))
			//require.Equal(t, retrievedValue, storeValue)

			m.ShutdownStores()
			m.Shutdown()

			logger.Infof("cleaned up all successfully")
		},
	)
}
