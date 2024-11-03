package backup_restore

//import (
//	"context"
//	"os/exec"
//	"testing"
//	"time"
//
//	"github.com/dgraph-io/badger/v4"
//	"github.com/stretchr/testify/require"
//
//	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
//	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
//
//	badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/manager"
//	badgerdb_operations_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/transactions/operations"
//	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v4/management"
//	badgerdb_operation_models_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v4/operation"
//	chainded_operator "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator"
//)
//
//const backupFile = "./db.bak"
//
//func Test_Integration_BackupFlowRAW(t *testing.T) {
//	ctx, _ := context.WithCancel(context.Background())
//	logger := go_logger.NewGoLogger(ctx, nil, &go_logger.Opts{Debug: true}).FromCtx(ctx)
//
//	err := exec.Command("rm", "-rf", ".db").Run()
//	require.NoError(t, err)
//
//	t.Run("backupRAW", func(t *testing.T) {
//		logger.Infof("opening badger local manager")
//		db, err := badger.Open(badger.DefaultOptions(badgerdb_manager_adaptor_v4.InternalLocalManagement))
//		require.NoError(t, err)
//
//		serializer := go_serializer.NewJsonSerializer()
//		chainedOperator := chainded_operator.NewChainOperator()
//
//		managerParams := &badgerdb_manager_adaptor_v4.BadgerLocalManagerV4Params{
//			DB:         db,
//			Logger:     logger,
//			Serializer: serializer,
//		}
//		m, err := badgerdb_manager_adaptor_v4.NewBadgerLocalManagerV4(managerParams)
//		require.NoError(t, err)
//		operatorParams := &badgerdb_operations_adaptor_v4.BadgerOperatorV4Params{
//			Manager:         m,
//			Serializer:      serializer,
//			ChainedOperator: chainedOperator,
//		}
//		op, err := badgerdb_operations_adaptor_v4.NewBadgerOperatorV4(operatorParams)
//		require.NoError(t, err)
//
//		logger.Debugf("starting manager")
//		err = m.Start()
//		require.NoError(t, err)
//		logger.Debugf("manager started")
//
//		logger.Debugf("creating store")
//		dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
//			Name:         "operations-db-integration-test",
//			Path:         "operations-db-integration-test",
//			CreatedAt:    time.Now(),
//			LastOpenedAt: time.Now(),
//		}
//		err = m.CreateStore(ctx, dbInfoOpTest)
//		require.NoError(t, err)
//		logger.Debugf("store created")
//
//		dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
//		require.NoError(t, err)
//
//		storeKey := []byte("key-test-to-store")
//		storeValue := []byte("value test to store")
//		logger.Debugf("writing value")
//		err = op.
//			Operate(dbMemoryInfo).
//			Create(
//				ctx,
//				&badgerdb_operation_models_v4.Item{
//					Key:   storeKey,
//					Value: storeValue,
//				},
//				&badgerdb_operation_models_v4.IndexOpts{},
//				chainded_operator.DefaultRetrialOps,
//			)
//		require.NoError(t, err)
//		logger.Debugf("value written")
//
//		logger.Debugf("loading value")
//		retrievedValue, err := op.Operate(dbMemoryInfo).Load(
//			ctx,
//			&badgerdb_operation_models_v4.Item{
//				Key:   storeKey,
//				Value: storeValue,
//			},
//			&badgerdb_operation_models_v4.IndexOpts{},
//		)
//		require.NoError(t, err)
//		logger.Debugf("value loaded")
//		t.Log("retrieved value ->", string(retrievedValue))
//
//		storeKey2 := []byte("key-test-to-store-2")
//		storeValue2 := []byte("value test to store-2")
//		logger.Debugf("writing value")
//		err = op.
//			Operate(dbMemoryInfo).
//			Create(
//				ctx,
//				&badgerdb_operation_models_v4.Item{
//					Key:   storeKey2,
//					Value: storeValue2,
//				},
//				&badgerdb_operation_models_v4.IndexOpts{},
//				chainded_operator.DefaultRetrialOps,
//			)
//		require.NoError(t, err)
//		logger.Debugf("value written")
//
//		logger.Debugf("loading value")
//		retrievedValue2, err := op.Operate(dbMemoryInfo).Load(
//			ctx,
//			&badgerdb_operation_models_v4.Item{
//				Key:   storeKey2,
//				Value: storeValue2,
//			},
//			&badgerdb_operation_models_v4.IndexOpts{},
//		)
//		require.NoError(t, err)
//		logger.Debugf("value loaded")
//		t.Log("retrieved value ->", string(retrievedValue2))
//
//		retrievedValues, err := op.Operate(dbMemoryInfo).List(
//			ctx,
//			&badgerdb_operation_models_v4.IndexOpts{
//				IndexProperties: badgerdb_operation_models_v4.IndexProperties{
//					ListSearchPattern: badgerdb_operation_models_v4.All,
//				},
//			},
//			&badgerdb_management_models_v4.Pagination{
//				PageID:   0,
//				PageSize: 0,
//			},
//		)
//		require.NoError(t, err)
//		t.Log(retrievedValues)
//		for _, item := range retrievedValues {
//			t.Log(string(item.Key), string(item.Value))
//		}
//
//		m.ShutdownStores()
//		m.Shutdown()
//
//		err = BackupRAW(ctx, backupFile)
//		require.NoError(t, err)
//	})
//
//	err = exec.Command("rm", "-rf", ".db").Run()
//	require.NoError(t, err)
//
//	t.Run("restoreRAW", func(t *testing.T) {
//		err := RestoreRAW(ctx, backupFile)
//		require.NoError(t, err)
//		logger.Debugf("backup restored")
//
//		logger.Infof("opening badger local manager")
//		db, err := badger.Open(badger.DefaultOptions(badgerdb_manager_adaptor_v4.InternalLocalManagement))
//		require.NoError(t, err)
//
//		serializer := go_serializer.NewJsonSerializer()
//		chainedOperator := chainded_operator.NewChainOperator()
//
//		managerParams := &badgerdb_manager_adaptor_v4.BadgerLocalManagerV4Params{
//			DB:         db,
//			Logger:     logger,
//			Serializer: serializer,
//		}
//		m, err := badgerdb_manager_adaptor_v4.NewBadgerLocalManagerV4(managerParams)
//		require.NoError(t, err)
//		operatorParams := &badgerdb_operations_adaptor_v4.BadgerOperatorV4Params{
//			Manager:         m,
//			Serializer:      serializer,
//			ChainedOperator: chainedOperator,
//		}
//		op, err := badgerdb_operations_adaptor_v4.NewBadgerOperatorV4(operatorParams)
//		require.NoError(t, err)
//
//		logger.Debugf("starting manager")
//		err = m.Start()
//		require.NoError(t, err)
//		logger.Debugf("manager started")
//
//		dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
//			Name: "operations-db-integration-test",
//		}
//		dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
//		require.NoError(t, err)
//		t.Log(dbMemoryInfo)
//
//		info, err := m.ListStoreInfo(ctx, 0, 0)
//		require.NoError(t, err)
//		t.Logf("%v", len(info))
//		t.Logf("%v", info[0])
//		t.Logf("%v", info[1])
//		t.Logf("%v", info[2])
//
//		retrievedValues, err := op.Operate(dbMemoryInfo).List(
//			ctx,
//			&badgerdb_operation_models_v4.IndexOpts{
//				IndexProperties: badgerdb_operation_models_v4.IndexProperties{
//					ListSearchPattern: badgerdb_operation_models_v4.All,
//				},
//			},
//			&badgerdb_management_models_v4.Pagination{},
//		)
//		require.NoError(t, err)
//		t.Log(retrievedValues)
//		for _, item := range retrievedValues {
//			t.Log(string(item.Key), string(item.Value))
//		}
//
//		storeKey := []byte("key-test-to-store")
//		storeValue := []byte("value test to store")
//
//		logger.Debugf("loading value")
//		retrievedValue, err := op.Operate(dbMemoryInfo).Load(
//			ctx,
//			&badgerdb_operation_models_v4.Item{
//				Key:   storeKey,
//				Value: storeValue,
//			},
//			&badgerdb_operation_models_v4.IndexOpts{},
//		)
//		require.NoError(t, err)
//		logger.Debugf("value loaded")
//		t.Log("retrieved value ->", string(retrievedValue))
//
//		storeKey2 := []byte("key-test-to-store-2")
//		storeValue2 := []byte("value test to store-2")
//
//		logger.Debugf("loading value")
//		retrievedValue2, err := op.Operate(dbMemoryInfo).Load(
//			ctx,
//			&badgerdb_operation_models_v4.Item{
//				Key:   storeKey2,
//				Value: storeValue2,
//			},
//			&badgerdb_operation_models_v4.IndexOpts{},
//		)
//		require.NoError(t, err)
//		logger.Debugf("value loaded")
//		t.Log("retrieved value ->", string(retrievedValue2))
//	})
//}
