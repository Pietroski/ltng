package operations

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"
	manager "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager"
	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
)

func Test_Integration_Create_Load(t *testing.T) {
	t.Run(
		"",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
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
			op := NewBadgerOperator(m, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			if err := m.CreateOpenStoreAndLoadIntoMemory(dbInfoOpTest); err != nil {
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

			dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
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
			if err := op.Operate(dbMemoryInfo).Create(storeKey, storeValue); err != nil {
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

			retrievedValue, err := op.Operate(dbMemoryInfo).Load(storeKey)
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

			m.ShutdownStores()
			m.Shutdown()

			// TODO: list opened stores

			logger.Infof("cleaned up all successfully")
		},
	)
}

func Test_Integration_Create_Load_Delete_Load(t *testing.T) {
	t.Run(
		"",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			require.NoError(t, err)

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			op := NewBadgerOperator(m, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateOpenStoreAndLoadIntoMemory(dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			err = op.Operate(dbMemoryInfo).Create(storeKey, storeValue)
			require.NoError(t, err)

			retrievedValue, err := op.Operate(dbMemoryInfo).Load(storeKey)
			require.NoError(t, err)

			t.Log("retrieved value ->", string(retrievedValue))

			err = op.Operate(dbMemoryInfo).Delete(storeKey)
			require.NoError(t, err)

			retrievedValue, err = op.Operate(dbMemoryInfo).Load(storeKey)
			require.Error(t, err)
			require.Equal(t, "Key not found", err.Error())
			t.Log("expected err ->", err)
			t.Log("retrieved value ->", string(retrievedValue))

			m.ShutdownStores()
			m.Shutdown()

			// TODO: list opened stores

			logger.Infof("cleaned up all successfully")
		},
	)
}

func Test_Integration_Create_List_All(t *testing.T) {
	t.Run(
		"",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			require.NoError(t, err)

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			op := NewBadgerOperator(m, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateOpenStoreAndLoadIntoMemory(dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			storeKey1 := []byte("key-test-to-store-1")
			storeValue1 := []byte("value test to store-1")
			storeKey2 := []byte("key-test-to-store-2")
			storeValue2 := []byte("value test to store-2")
			storeKey3 := []byte("key-test-to-store-3")
			storeValue3 := []byte("value test to store-3")
			storeKey4 := []byte("key-test-to-store-4")
			storeValue4 := []byte("value test to store-4")
			storeKey5 := []byte("key-test-to-store-5")
			storeValue5 := []byte("value test to store-5")
			err = op.Operate(dbMemoryInfo).Create(storeKey1, storeValue1)
			require.NoError(t, err)
			err = op.Operate(dbMemoryInfo).Create(storeKey2, storeValue2)
			require.NoError(t, err)
			err = op.Operate(dbMemoryInfo).Create(storeKey3, storeValue3)
			require.NoError(t, err)
			err = op.Operate(dbMemoryInfo).Create(storeKey4, storeValue4)
			require.NoError(t, err)
			err = op.Operate(dbMemoryInfo).Create(storeKey5, storeValue5)
			require.NoError(t, err)

			retrievedValues, err := op.Operate(dbMemoryInfo).ListAll()
			require.NoError(t, err)

			bs, err := json.MarshalIndent(retrievedValues, "", "  ")
			require.NoError(t, err)
			t.Log("retrieved value ->", string(bs))

			m.ShutdownStores()
			m.Shutdown()

			// TODO: list opened stores

			logger.Infof("cleaned up all successfully")
		},
	)
}

func Test_Integration_Create_List_Paginated(t *testing.T) {
	t.Run(
		"",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := go_logger.FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(manager.InternalLocalManagement))
			require.NoError(t, err)

			serializer := go_serializer.NewJsonSerializer()

			m := manager.NewBadgerLocalManager(db, serializer, logger)
			op := NewBadgerOperator(m, serializer)

			logger.Debugf("creating store")
			dbInfoOpTest := &management_models.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateOpenStoreAndLoadIntoMemory(dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			storeKey1 := []byte("key-test-to-store-1")
			storeValue1 := []byte("value test to store-1")
			storeKey2 := []byte("key-test-to-store-2")
			storeValue2 := []byte("value test to store-2")
			storeKey3 := []byte("key-test-to-store-3")
			storeValue3 := []byte("value test to store-3")
			storeKey4 := []byte("key-test-to-store-4")
			storeValue4 := []byte("value test to store-4")
			storeKey5 := []byte("key-test-to-store-5")
			storeValue5 := []byte("value test to store-5")
			err = op.Operate(dbMemoryInfo).Create(storeKey1, storeValue1)
			require.NoError(t, err)
			err = op.Operate(dbMemoryInfo).Create(storeKey2, storeValue2)
			require.NoError(t, err)
			err = op.Operate(dbMemoryInfo).Create(storeKey3, storeValue3)
			require.NoError(t, err)
			err = op.Operate(dbMemoryInfo).Create(storeKey4, storeValue4)
			require.NoError(t, err)
			err = op.Operate(dbMemoryInfo).Create(storeKey5, storeValue5)
			require.NoError(t, err)

			pagination := &management_models.PaginationRequest{
				PageID:   2,
				PageSize: 2,
			}
			retrievedValues, err := op.Operate(dbMemoryInfo).ListPaginated(pagination)
			require.NoError(t, err)
			require.Len(t, retrievedValues, 2)
			require.Equal(t, storeKey3, retrievedValues[0].Key)
			require.Equal(t, storeValue3, retrievedValues[0].Value)
			require.Equal(t, storeKey4, retrievedValues[1].Key)
			require.Equal(t, storeValue4, retrievedValues[1].Value)

			bs, err := json.MarshalIndent(retrievedValues, "", "  ")
			require.NoError(t, err)
			t.Log("retrieved value ->", string(bs))

			m.ShutdownStores()
			m.Shutdown()

			// TODO: list opened stores

			logger.Infof("cleaned up all successfully")
		},
	)
}
