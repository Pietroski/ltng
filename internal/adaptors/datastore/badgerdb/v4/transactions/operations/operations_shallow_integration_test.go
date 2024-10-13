package badgerdb_operations_adaptor_v4

import (
	"bytes"
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_random "gitlab.com/pietroski-software-company/tools/random/go-random/pkg/tools/random"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"

	badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/manager"
	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v4/management"
	badgerdb_operation_models_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v4/operation"
	chainded_operator "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator"
	list_operator "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/list-operator"
)

var debugMode = false

func Test_Integration_Create_Load_Delete(t *testing.T) {
	err := exec.Command("rm", "-rf", ".db").Run()
	require.NoError(t, err)

	t.Run(
		"happy path",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := go_logger.NewGoLogger(ctx, nil, &go_logger.Opts{Debug: debugMode}).FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(badgerdb_manager_adaptor_v4.InternalLocalManagement))
			require.NoError(t, err)

			m, err := badgerdb_manager_adaptor_v4.NewBadgerLocalManagerV4(ctx, badgerdb_manager_adaptor_v4.WithDB(db))
			require.NoError(t, err)

			op, err := NewBadgerOperatorV4(ctx, WithManager(m))
			require.NoError(t, err)

			logger.Debugf("starting manager")
			err = m.Start()
			require.NoError(t, err)
			logger.Debugf("manager started")

			logger.Debugf("creating store")
			dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			logger.Debugf("writing value")
			err = op.
				Operate(dbMemoryInfo).
				Create(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
					list_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Debugf("value written")

			logger.Debugf("loading value")
			retrievedValue, err := op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Debugf("value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Debugf("deleting value")
			err = op.
				Operate(dbMemoryInfo).
				Delete(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
					chainded_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Debugf("value deleted")

			logger.Debugf("loading value")
			retrievedValue, err = op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Debugf("value loaded")

			logger.Debugf("deleting store")
			err = m.DeleteStore(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)
			logger.Debugf("store deleted")

			logger.Debugf("shutting down stores")
			m.ShutdownStores()
			m.Shutdown()
			logger.Debugf("stores shut down")

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"happy path - call with upsert",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := go_logger.NewGoLogger(ctx, nil, &go_logger.Opts{Debug: debugMode}).FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(badgerdb_manager_adaptor_v4.InternalLocalManagement))
			require.NoError(t, err)

			m, err := badgerdb_manager_adaptor_v4.NewBadgerLocalManagerV4(ctx, badgerdb_manager_adaptor_v4.WithDB(db))
			require.NoError(t, err)

			op, err := NewBadgerOperatorV4(ctx, WithManager(m))
			require.NoError(t, err)

			logger.Debugf("starting manager")
			err = m.Start()
			require.NoError(t, err)
			logger.Debugf("manager started")

			logger.Debugf("creating store")
			dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			logger.Debugf("writing value")
			err = op.
				Operate(dbMemoryInfo).
				Create(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
					list_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Debugf("value written")

			logger.Debugf("re-writing value")
			err = op.
				Operate(dbMemoryInfo).
				Upsert(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
					list_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Debugf("value re-written")

			logger.Debugf("loading value")
			retrievedValue, err := op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Debugf("value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Debugf("deleting value")
			err = op.
				Operate(dbMemoryInfo).
				Delete(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
					chainded_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Debugf("value deleted")

			logger.Debugf("loading value")
			retrievedValue, err = op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Debugf("value loaded")

			logger.Debugf("deleting store")
			err = m.DeleteStore(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)
			logger.Debugf("store deleted")

			logger.Debugf("shutting down stores")
			m.ShutdownStores()
			m.Shutdown()
			logger.Debugf("stores shut down")

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"happy path - delete cascade",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := go_logger.NewGoLogger(ctx, nil, &go_logger.Opts{Debug: debugMode}).FromCtx(ctx)
			serializer := go_serializer.NewJsonSerializer()

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(badgerdb_manager_adaptor_v4.InternalLocalManagement))
			require.NoError(t, err)

			m, err := badgerdb_manager_adaptor_v4.NewBadgerLocalManagerV4(ctx, badgerdb_manager_adaptor_v4.WithDB(db))
			require.NoError(t, err)

			op, err := NewBadgerOperatorV4(ctx, WithManager(m))
			require.NoError(t, err)

			logger.Debugf("starting manager")
			err = m.Start()
			require.NoError(t, err)
			logger.Debugf("manager started")

			logger.Debugf("creating store")
			dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			newUUID, err := uuid.NewRandom()
			require.NoError(t, err)
			type Person struct {
				ID       uuid.UUID `json:"id"`
				Name     string    `json:"name"`
				Email    string    `json:"email"`
				Username string    `json:"username"`
			}
			person := &Person{
				ID:       newUUID,
				Name:     go_random.RandomString(8),
				Email:    go_random.RandomEmail(),
				Username: go_random.RandomString(8),
			}

			key := newUUID
			value := person
			emailKey, err := serializer.Serialize(person.Email)
			require.NoError(t, err)
			usernameKey, err := serializer.Serialize(person.Username)
			require.NoError(t, err)
			storeKey, err := serializer.Serialize(key)
			require.NoError(t, err)
			storeValue, err := serializer.Serialize(value)
			require.NoError(t, err)
			logger.Debugf("writing value")
			err = op.
				Operate(dbMemoryInfo).
				Create(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:          true,
						ParentKey:       storeKey,
						IndexingKeys:    [][]byte{emailKey, usernameKey},
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{},
					},
					list_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Debugf("value written")

			logger.Debugf("loading value")
			retrievedValue, err := op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Debugf("value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Debugf("loading value for indexed list")
			indexedListName := dbMemoryInfo.Name + badgerdb_manager_adaptor_v4.IndexedListSuffixName
			idxListMemoryInfo, err := m.GetDBMemoryInfo(ctx, indexedListName)
			retrievedValue, err = op.Operate(idxListMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Debugf("value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Debugf("deleting value")
			err = op.
				Operate(dbMemoryInfo).
				Delete(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key: storeKey,
					},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx: true,
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexDeletionBehaviour: badgerdb_operation_models_v4.Cascade,
						},
					},
					chainded_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Debugf("value deleted")

			logger.Debugf("loading value")
			retrievedValue, err = op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Debugf("value loaded")

			logger.Debugf("loading value for indexed list")
			indexedListName = dbMemoryInfo.Name + badgerdb_manager_adaptor_v4.IndexedListSuffixName
			idxListMemoryInfo, err = m.GetDBMemoryInfo(ctx, indexedListName)
			retrievedValue, err = op.Operate(idxListMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Debugf("value loaded")

			logger.Debugf("deleting store")
			err = m.DeleteStore(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)
			logger.Debugf("store deleted")

			logger.Debugf("shutting down stores")
			m.ShutdownStores()
			m.Shutdown()
			logger.Debugf("stores shut down")

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"happy path - delete cascade by index",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := go_logger.NewGoLogger(ctx, nil, &go_logger.Opts{Debug: debugMode}).FromCtx(ctx)
			serializer := go_serializer.NewJsonSerializer()

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(badgerdb_manager_adaptor_v4.InternalLocalManagement))
			require.NoError(t, err)

			m, err := badgerdb_manager_adaptor_v4.NewBadgerLocalManagerV4(ctx, badgerdb_manager_adaptor_v4.WithDB(db))
			require.NoError(t, err)

			op, err := NewBadgerOperatorV4(ctx, WithManager(m))
			require.NoError(t, err)

			logger.Debugf("starting manager")
			err = m.Start()
			require.NoError(t, err)
			logger.Debugf("manager started")

			logger.Debugf("creating store")
			dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			newUUID, err := uuid.NewRandom()
			require.NoError(t, err)
			type Person struct {
				ID       uuid.UUID `json:"id"`
				Name     string    `json:"name"`
				Email    string    `json:"email"`
				Username string    `json:"username"`
			}
			person := &Person{
				ID:       newUUID,
				Name:     go_random.RandomString(8),
				Email:    go_random.RandomEmail(),
				Username: go_random.RandomString(8),
			}

			key := newUUID
			value := person
			emailKey, err := serializer.Serialize(person.Email)
			require.NoError(t, err)
			usernameKey, err := serializer.Serialize(person.Username)
			require.NoError(t, err)
			storeKey, err := serializer.Serialize(key)
			require.NoError(t, err)
			storeValue, err := serializer.Serialize(value)
			require.NoError(t, err)
			logger.Debugf("writing value")
			err = op.
				Operate(dbMemoryInfo).
				Create(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:          true,
						ParentKey:       storeKey,
						IndexingKeys:    [][]byte{emailKey, usernameKey},
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{},
					},
					list_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Debugf("value written")

			logger.Debugf("loading value")
			retrievedValue, err := op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Debugf("value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Debugf("loading value for indexed list")
			indexedListName := dbMemoryInfo.Name + badgerdb_manager_adaptor_v4.IndexedListSuffixName
			idxListMemoryInfo, err := m.GetDBMemoryInfo(ctx, indexedListName)
			retrievedValue, err = op.Operate(idxListMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Debugf("value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Debugf("deleting value")
			err = op.
				Operate(dbMemoryInfo).
				Delete(
					ctx,
					&badgerdb_operation_models_v4.Item{},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx: true,
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexDeletionBehaviour: badgerdb_operation_models_v4.CascadeByIdx,
						},
						IndexingKeys: [][]byte{emailKey},
					},
					chainded_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Debugf("value deleted")

			logger.Debugf("loading value")
			retrievedValue, err = op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Debugf("value loaded")

			logger.Debugf("loading value for indexed list")
			indexedListName = dbMemoryInfo.Name + badgerdb_manager_adaptor_v4.IndexedListSuffixName
			idxListMemoryInfo, err = m.GetDBMemoryInfo(ctx, indexedListName)
			retrievedValue, err = op.Operate(idxListMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Debugf("value loaded")

			logger.Debugf("deleting store")
			err = m.DeleteStore(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)
			logger.Debugf("store deleted")

			logger.Debugf("shutting down stores")
			m.ShutdownStores()
			m.Shutdown()
			logger.Debugf("stores shut down")

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"happy path - delete cascade - call with upsert",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := go_logger.NewGoLogger(ctx, nil, &go_logger.Opts{Debug: debugMode}).FromCtx(ctx)
			serializer := go_serializer.NewJsonSerializer()

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(badgerdb_manager_adaptor_v4.InternalLocalManagement))
			require.NoError(t, err)

			m, err := badgerdb_manager_adaptor_v4.NewBadgerLocalManagerV4(ctx, badgerdb_manager_adaptor_v4.WithDB(db))
			require.NoError(t, err)

			op, err := NewBadgerOperatorV4(ctx, WithManager(m))
			require.NoError(t, err)

			logger.Debugf("starting manager")
			err = m.Start()
			require.NoError(t, err)
			logger.Debugf("manager started")

			logger.Debugf("creating store")
			dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			newUUID, err := uuid.NewRandom()
			require.NoError(t, err)
			type Person struct {
				ID       uuid.UUID `json:"id"`
				Name     string    `json:"name"`
				Email    string    `json:"email"`
				Username string    `json:"username"`
			}
			person := &Person{
				ID:       newUUID,
				Name:     go_random.RandomString(8),
				Email:    go_random.RandomEmail(),
				Username: go_random.RandomString(8),
			}

			key := newUUID
			value := person
			emailKey, err := serializer.Serialize(person.Email)
			require.NoError(t, err)
			usernameKey, err := serializer.Serialize(person.Username)
			require.NoError(t, err)
			storeKey, err := serializer.Serialize(key)
			require.NoError(t, err)
			storeValue, err := serializer.Serialize(value)
			require.NoError(t, err)
			logger.Debugf("writing value")
			err = op.
				Operate(dbMemoryInfo).
				Upsert(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:          true,
						ParentKey:       storeKey,
						IndexingKeys:    [][]byte{emailKey, usernameKey},
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{},
					},
					list_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Debugf("value written")

			logger.Debugf("loading value")
			retrievedValue, err := op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Debugf("value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Debugf("loading value for indexed list")
			indexedListName := dbMemoryInfo.Name + badgerdb_manager_adaptor_v4.IndexedListSuffixName
			idxListMemoryInfo, err := m.GetDBMemoryInfo(ctx, indexedListName)
			retrievedValue, err = op.Operate(idxListMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Debugf("value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Debugf("deleting value")
			err = op.
				Operate(dbMemoryInfo).
				Delete(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key: storeKey,
					},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx: true,
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexDeletionBehaviour: badgerdb_operation_models_v4.Cascade,
						},
					},
					chainded_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Debugf("value deleted")

			logger.Debugf("loading value")
			retrievedValue, err = op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Debugf("value loaded")

			logger.Debugf("loading value for indexed list")
			indexedListName = dbMemoryInfo.Name + badgerdb_manager_adaptor_v4.IndexedListSuffixName
			idxListMemoryInfo, err = m.GetDBMemoryInfo(ctx, indexedListName)
			retrievedValue, err = op.Operate(idxListMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Debugf("value loaded")

			logger.Debugf("deleting store")
			err = m.DeleteStore(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)
			logger.Debugf("store deleted")

			logger.Debugf("shutting down stores")
			m.ShutdownStores()
			m.Shutdown()
			logger.Debugf("stores shut down")

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"happy path - delete index only - call with upsert",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := go_logger.NewGoLogger(ctx, nil, &go_logger.Opts{Debug: debugMode}).FromCtx(ctx)
			serializer := go_serializer.NewJsonSerializer()

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(badgerdb_manager_adaptor_v4.InternalLocalManagement))
			require.NoError(t, err)

			m, err := badgerdb_manager_adaptor_v4.NewBadgerLocalManagerV4(ctx, badgerdb_manager_adaptor_v4.WithDB(db))
			require.NoError(t, err)

			op, err := NewBadgerOperatorV4(ctx, WithManager(m))
			require.NoError(t, err)

			logger.Debugf("starting manager")
			err = m.Start()
			require.NoError(t, err)
			logger.Debugf("manager started")

			logger.Debugf("creating store")
			dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			storeList, err := m.ListStoreMemoryInfo(ctx, 0, 0)
			require.NoError(t, err)
			logger.Debugf("stores", go_logger.Field{"stores": storeList})

			dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			newUUID, err := uuid.NewRandom()
			require.NoError(t, err)
			type Person struct {
				ID       uuid.UUID `json:"id"`
				Name     string    `json:"name"`
				Email    string    `json:"email"`
				Username string    `json:"username"`
			}
			person := &Person{
				ID:       newUUID,
				Name:     go_random.RandomString(8),
				Email:    go_random.RandomEmail(),
				Username: go_random.RandomString(8),
			}

			key := newUUID
			value := person
			emailKey, err := serializer.Serialize(person.Email)
			require.NoError(t, err)
			usernameKey, err := serializer.Serialize(person.Username)
			require.NoError(t, err)
			storeKey, err := serializer.Serialize(key)
			require.NoError(t, err)
			storeValue, err := serializer.Serialize(value)
			require.NoError(t, err)
			// UPSERT
			{
				logger.Debugf("writing value")
				err = op.
					Operate(dbMemoryInfo).
					Upsert(
						ctx,
						&badgerdb_operation_models_v4.Item{
							Key:   storeKey,
							Value: storeValue,
						},
						&badgerdb_operation_models_v4.IndexOpts{
							HasIdx:          true,
							ParentKey:       storeKey,
							IndexingKeys:    [][]byte{emailKey, usernameKey},
							IndexProperties: badgerdb_operation_models_v4.IndexProperties{},
						},
						list_operator.DefaultRetrialOps,
					)
				require.NoError(t, err)
				logger.Debugf("value written")
			}

			// value from main key
			{
				logger.Debugf("loading value")
				retrievedValue, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key: storeKey,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
				)
				require.NoError(t, err)
				logger.Debugf("value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				require.Equal(t, storeValue, retrievedValue)
			}

			// indexed list
			{
				logger.Debugf("loading value for indexed list")
				retrievedValue, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:    true,
						ParentKey: storeKey,
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexSearchPattern: badgerdb_operation_models_v4.IndexingList,
						},
					},
				)
				require.NoError(t, err)
				logger.Debugf("value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				result := bytes.Join([][]byte{emailKey, usernameKey}, []byte(bsSeparator))
				require.Equal(t, result, retrievedValue)
			}

			// indexed list
			{
				logger.Debugf("loading value for indexed list")
				retrievedValue, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:       true,
						IndexingKeys: [][]byte{emailKey},
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexSearchPattern: badgerdb_operation_models_v4.IndexingList,
						},
					},
				)
				require.NoError(t, err)
				logger.Debugf("value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				result := bytes.Join([][]byte{emailKey, usernameKey}, []byte(bsSeparator))
				require.Equal(t, result, retrievedValue)
			}

			// value from index item - AndComputational
			{
				logger.Debugf("loading value from index item")
				retrievedValue, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    nil,
						IndexingKeys: [][]byte{usernameKey},
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexSearchPattern: badgerdb_operation_models_v4.AndComputational,
						},
					},
				)
				require.NoError(t, err)
				logger.Debugf("value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				require.Equal(t, storeValue, retrievedValue)
			}

			// value from index item - OrComputational
			{
				logger.Debugf("loading value from index item")
				retrievedValue, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    nil,
						IndexingKeys: [][]byte{usernameKey},
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexSearchPattern: badgerdb_operation_models_v4.OrComputational,
						},
					},
				)
				require.NoError(t, err)
				logger.Debugf("value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				require.Equal(t, storeValue, retrievedValue)
			}

			// value from index item - and computational - two items
			{
				logger.Debugf("loading value for index item")
				retrievedValue, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    nil,
						IndexingKeys: [][]byte{emailKey, usernameKey},
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexSearchPattern: badgerdb_operation_models_v4.AndComputational,
						},
					},
				)
				require.NoError(t, err)
				logger.Debugf("value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				require.Equal(t, storeValue, retrievedValue)
			}

			// value from index item - or computational - two items
			{
				logger.Debugf("loading value for index item")
				retrievedValue, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    nil,
						IndexingKeys: [][]byte{emailKey, usernameKey},
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexSearchPattern: badgerdb_operation_models_v4.OrComputational,
						},
					},
				)
				require.NoError(t, err)
				logger.Debugf("value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				require.Equal(t, storeValue, retrievedValue)
			}

			// DELETE - single key
			{
				logger.Debugf("deleting value")
				err = op.
					Operate(dbMemoryInfo).
					Delete(
						ctx,
						&badgerdb_operation_models_v4.Item{},
						&badgerdb_operation_models_v4.IndexOpts{
							HasIdx: true,
							IndexProperties: badgerdb_operation_models_v4.IndexProperties{
								IndexDeletionBehaviour: badgerdb_operation_models_v4.IndexOnly,
							},
							IndexingKeys: [][]byte{usernameKey},
						},
						chainded_operator.DefaultRetrialOps,
					)
				require.NoError(t, err)
				logger.Debugf("value deleted")
			}

			// value from main key
			{
				logger.Debugf("loading value")
				retrievedValue, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key: storeKey,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
				)
				require.NoError(t, err)
				logger.Debugf("value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				require.Equal(t, storeValue, retrievedValue)
			}

			// indexed list
			{
				logger.Debugf("loading value for indexed list")
				retrievedValue, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:    true,
						ParentKey: storeKey,
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexSearchPattern: badgerdb_operation_models_v4.IndexingList,
						},
					},
				)
				require.NoError(t, err)
				logger.Debugf("value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				result := bytes.Join([][]byte{emailKey}, []byte(bsSeparator))
				require.Equal(t, result, retrievedValue)
			}

			// value from index item - AndComputational
			{
				logger.Debugf("loading value from index item")
				retrievedValue, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    nil,
						IndexingKeys: [][]byte{usernameKey},
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexSearchPattern: badgerdb_operation_models_v4.AndComputational,
						},
					},
				)
				require.Error(t, err)
				logger.Debugf("value loaded")
				require.Equal(t, []byte{}, retrievedValue)
			}

			// value from index item - One
			{
				logger.Debugf("loading value from indexed item")
				_, err = op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:       true,
						IndexingKeys: [][]byte{usernameKey},
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexSearchPattern: badgerdb_operation_models_v4.One,
						},
					},
				)
				require.Error(t, err)
				logger.Debugf("value loaded")
			}

			// value from index item - One
			{
				logger.Debugf("loading value for indexed item")
				retrievedValue, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    nil,
						IndexingKeys: [][]byte{emailKey},
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexSearchPattern: badgerdb_operation_models_v4.One,
						},
					},
				)
				require.NoError(t, err)
				logger.Debugf("value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				require.Equal(t, storeValue, retrievedValue)
			}

			// DELETE - cascade
			{
				logger.Debugf("deleting value")
				err = op.
					Operate(dbMemoryInfo).
					Delete(
						ctx,
						&badgerdb_operation_models_v4.Item{
							Key: storeKey,
						},
						&badgerdb_operation_models_v4.IndexOpts{
							HasIdx: true,
							IndexProperties: badgerdb_operation_models_v4.IndexProperties{
								IndexDeletionBehaviour: badgerdb_operation_models_v4.Cascade,
							},
						},
						chainded_operator.DefaultRetrialOps,
					)
				require.NoError(t, err)
				logger.Debugf("value deleted")
			}

			// value from main key
			{
				logger.Debugf("loading value")
				_, err = op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key: storeKey,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
				)
				require.Error(t, err)
				logger.Debugf("value loaded")
			}

			// indexed list
			{
				logger.Debugf("loading value for indexed list")
				retrievedValue, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:    true,
						ParentKey: storeKey,
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexSearchPattern: badgerdb_operation_models_v4.IndexingList,
						},
					},
				)
				require.Error(t, err)
				logger.Debugf("value loaded")
				require.Equal(t, []byte(nil), retrievedValue)
			}

			logger.Debugf("deleting store")
			err = m.DeleteStore(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)
			logger.Debugf("store deleted")

			logger.Debugf("shutting down stores")
			m.ShutdownStores()
			m.Shutdown()
			logger.Debugf("stores shut down")

			logger.Infof("cleaned up all successfully")
		},
	)

	t.Run(
		"item already created",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := go_logger.NewGoLogger(ctx, nil, &go_logger.Opts{Debug: debugMode}).FromCtx(ctx)

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(badgerdb_manager_adaptor_v4.InternalLocalManagement))
			require.NoError(t, err)

			m, err := badgerdb_manager_adaptor_v4.NewBadgerLocalManagerV4(ctx, badgerdb_manager_adaptor_v4.WithDB(db))
			require.NoError(t, err)

			op, err := NewBadgerOperatorV4(ctx, WithManager(m))
			require.NoError(t, err)

			logger.Debugf("starting manager")
			err = m.Start()
			require.NoError(t, err)
			logger.Debugf("manager started")

			logger.Debugf("creating store")
			dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			logger.Debugf("writing value")
			err = op.
				Operate(dbMemoryInfo).
				Create(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
					list_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Debugf("value written")

			logger.Debugf("writing value")
			err = op.
				Operate(dbMemoryInfo).
				Create(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
					list_operator.DefaultRetrialOps,
				)
			require.Error(t, err)
			logger.Debugf("value written")

			logger.Debugf("loading value")
			retrievedValue, err := op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Debugf("value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Debugf("deleting value")
			err = op.
				Operate(dbMemoryInfo).
				Delete(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
					chainded_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Debugf("value deleted")

			logger.Debugf("loading value")
			retrievedValue, err = op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Debugf("value loaded")

			logger.Debugf("deleting store")
			err = m.DeleteStore(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)
			logger.Debugf("store deleted")

			logger.Debugf("shutting down stores")
			m.ShutdownStores()
			m.Shutdown()
			logger.Debugf("stores shut down")

			logger.Infof("cleaned up all successfully")
		},
	)
}

func Test_Integration_Create_Multiples_List_Delete(t *testing.T) {
	err := exec.Command("rm", "-rf", ".db").Run()
	require.NoError(t, err)

	t.Run(
		"happy path - Create_Multiples_List_Delete",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := go_logger.NewGoLogger(ctx, nil, &go_logger.Opts{Debug: debugMode}).FromCtx(ctx)
			serializer := go_serializer.NewJsonSerializer()

			logger.Infof("opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(badgerdb_manager_adaptor_v4.InternalLocalManagement))
			require.NoError(t, err)

			m, err := badgerdb_manager_adaptor_v4.NewBadgerLocalManagerV4(ctx, badgerdb_manager_adaptor_v4.WithDB(db))
			require.NoError(t, err)

			op, err := NewBadgerOperatorV4(ctx, WithManager(m))
			require.NoError(t, err)

			logger.Debugf("starting manager")
			err = m.Start()
			require.NoError(t, err)
			logger.Debugf("manager started")

			logger.Debugf("creating store")
			dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Debugf("store created")

			storeList, err := m.ListStoreMemoryInfo(ctx, 0, 0)
			require.NoError(t, err)
			logger.Debugf("stores", go_logger.Field{"stores": storeList})

			dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			type Person struct {
				ID       uuid.UUID `json:"id"`
				Name     string    `json:"name"`
				Email    string    `json:"email"`
				Username string    `json:"username"`
			}

			people := make([]*Person, 5)
			for idx, _ := range people {
				newUUID, err := uuid.NewRandom()
				require.NoError(t, err)
				person := &Person{
					ID:       newUUID,
					Name:     go_random.RandomString(8),
					Email:    go_random.RandomEmail(),
					Username: go_random.RandomString(8),
				}

				people[idx] = person
			}

			{
				for _, person := range people {
					key := person.ID
					value := person

					emailKey, err := serializer.Serialize(person.Email)
					require.NoError(t, err)
					usernameKey, err := serializer.Serialize(person.Username)
					require.NoError(t, err)
					storeKey, err := serializer.Serialize(key)
					require.NoError(t, err)
					storeValue, err := serializer.Serialize(value)
					require.NoError(t, err)

					// UPSERT
					{
						logger.Debugf("writing value")
						err = op.
							Operate(dbMemoryInfo).
							Upsert(
								ctx,
								&badgerdb_operation_models_v4.Item{
									Key:   storeKey,
									Value: storeValue,
								},
								&badgerdb_operation_models_v4.IndexOpts{
									HasIdx:          true,
									ParentKey:       storeKey,
									IndexingKeys:    [][]byte{emailKey, usernameKey},
									IndexProperties: badgerdb_operation_models_v4.IndexProperties{},
								},
								list_operator.DefaultRetrialOps,
							)
						require.NoError(t, err)
						logger.Debugf("value written")
					}
				}

				// list default
				{
					logger.Debugf("listing default pagination values")
					defaultPaginatedRetrievedValues, err := op.Operate(dbMemoryInfo).
						List(
							ctx,
							//&badgerdb_operation_models_v4.Item{},
							&badgerdb_operation_models_v4.IndexOpts{},
							&badgerdb_management_models_v4.Pagination{},
						)
					require.NoError(t, err)
					logger.Debugf("values loaded")
					t.Log("retrieved value ->", defaultPaginatedRetrievedValues)
				}

				// list all
				{
					logger.Debugf("listing all values")
					retrievedValues, err := op.Operate(dbMemoryInfo).
						List(
							ctx,
							//&badgerdb_operation_models_v4.Item{},
							&badgerdb_operation_models_v4.IndexOpts{
								IndexProperties: badgerdb_operation_models_v4.IndexProperties{
									ListSearchPattern: badgerdb_operation_models_v4.All,
								},
							},
							&badgerdb_management_models_v4.Pagination{},
						)
					require.NoError(t, err)
					logger.Debugf("values loaded")
					t.Log("retrieved value ->", retrievedValues)
				}

				// list paginated
				{
					logger.Debugf("listing values paginated")
					paginatedRetrievedValues, err := op.Operate(dbMemoryInfo).
						List(
							ctx,
							//&badgerdb_operation_models_v4.Item{},
							&badgerdb_operation_models_v4.IndexOpts{
								IndexProperties: badgerdb_operation_models_v4.IndexProperties{
									ListSearchPattern: badgerdb_operation_models_v4.All,
								},
							},
							&badgerdb_management_models_v4.Pagination{
								PageID:   2,
								PageSize: 2,
							},
						)
					require.NoError(t, err)
					logger.Debugf("values loaded")
					t.Log("retrieved value ->", paginatedRetrievedValues)
				}

				// list items
				{
					logger.Debugf("listing list items values")
					p0, err := serializer.Serialize(people[0].Email)
					require.NoError(t, err)
					p1, err := serializer.Serialize(people[1].Email)
					require.NoError(t, err)
					p4, err := serializer.Serialize(people[4].Email)
					require.NoError(t, err)
					listFromKeyValues, err := op.Operate(dbMemoryInfo).
						ListValuesFromIndexingKeys(
							ctx,
							&badgerdb_operation_models_v4.IndexOpts{
								IndexingKeys:    [][]byte{p0, p1, p4},
								IndexProperties: badgerdb_operation_models_v4.IndexProperties{},
							},
						)
					require.NoError(t, err)
					require.Len(t, listFromKeyValues, 3)
					t.Log("retrieved value ->", listFromKeyValues)
				}

				// list items
				{
					logger.Debugf("listing list items values")
					p0, err := serializer.Serialize(people[0].Email)
					require.NoError(t, err)
					p1, err := serializer.Serialize(people[1].Email)
					require.NoError(t, err)
					p4, err := serializer.Serialize(people[4].Email)
					require.NoError(t, err)
					listFromKeyValues, err := op.Operate(dbMemoryInfo).
						ListValuesFromIndexingKeys(
							ctx,
							&badgerdb_operation_models_v4.IndexOpts{
								IndexingKeys:    [][]byte{p0, p1, p4, []byte("does-not-exist")},
								IndexProperties: badgerdb_operation_models_v4.IndexProperties{},
							},
						)
					require.NoError(t, err)
					require.Len(t, listFromKeyValues, 4)
					for _, item := range listFromKeyValues {
						t.Log(item)
					}
					t.Log("retrieved value ->", listFromKeyValues)

				}

				for _, person := range people {
					key := person.ID
					storeKey, err := serializer.Serialize(key)
					require.NoError(t, err)

					// DELETE - cascade
					{
						err = op.Operate(dbMemoryInfo).Delete(
							ctx,
							&badgerdb_operation_models_v4.Item{
								Key: storeKey,
							},
							&badgerdb_operation_models_v4.IndexOpts{
								HasIdx: true,
								IndexProperties: badgerdb_operation_models_v4.IndexProperties{
									IndexDeletionBehaviour: badgerdb_operation_models_v4.Cascade,
								},
							},
							chainded_operator.DefaultRetrialOps,
						)
					}
				}

				// list all
				{
					logger.Debugf("listing all values")
					retrievedValues, err := op.Operate(dbMemoryInfo).
						List(
							ctx,
							//&badgerdb_operation_models_v4.Item{},
							&badgerdb_operation_models_v4.IndexOpts{
								IndexProperties: badgerdb_operation_models_v4.IndexProperties{
									ListSearchPattern: badgerdb_operation_models_v4.All,
								},
							},
							&badgerdb_management_models_v4.Pagination{},
						)
					require.NoError(t, err)
					logger.Debugf("values loaded")
					t.Log("retrieved value ->", retrievedValues)
				}
			}

			logger.Debugf("deleting store")
			err = m.DeleteStore(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)
			logger.Debugf("store deleted")

			logger.Debugf("shutting down stores")
			m.ShutdownStores()
			m.Shutdown()
			logger.Debugf("stores shut down")

			logger.Infof("cleaned up all successfully")
		},
	)
}

func Test_Integration_Create_Multiples_Load_Delete(t *testing.T) {
	err := exec.Command("rm", "-rf", ".db").Run()
	require.NoError(t, err)

	type TestStructure struct {
		Field1PK     string `json:"field1PK"`
		Field2       string `json:"field2"`
		Filed3       string `json:"filed3"`
		Field4IdxKey string `json:"field4IdxKey"`
		Field5IdxKey string `json:"field5IdxKey"`
	}

	ts := TestStructure{
		Field1PK:     go_random.RandomString(7),
		Field2:       go_random.RandomString(7),
		Filed3:       go_random.RandomString(7),
		Field4IdxKey: go_random.RandomString(7),
		Field5IdxKey: go_random.RandomString(7),
	}

	ctx, _ := context.WithCancel(context.Background())
	logger := go_logger.NewGoLogger(ctx, nil, &go_logger.Opts{Debug: debugMode}).FromCtx(ctx)
	serializer := go_serializer.NewJsonSerializer()

	logger.Infof("opening badger local manager")
	db, err := badger.Open(badger.DefaultOptions(badgerdb_manager_adaptor_v4.InternalLocalManagement))
	require.NoError(t, err)

	m, err := badgerdb_manager_adaptor_v4.NewBadgerLocalManagerV4(ctx, badgerdb_manager_adaptor_v4.WithDB(db))
	require.NoError(t, err)

	op, err := NewBadgerOperatorV4(ctx, WithManager(m))
	require.NoError(t, err)

	logger.Debugf("starting manager")
	err = m.Start()
	require.NoError(t, err)
	logger.Debugf("manager started")

	logger.Debugf("creating store")
	dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
		Name:         "operations-db-integration-test",
		Path:         "operations-db-integration-test",
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Now(),
	}
	err = m.CreateStore(ctx, dbInfoOpTest)
	require.NoError(t, err)
	logger.Debugf("store created")

	dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
	require.NoError(t, err)

	storeKey, err := serializer.Serialize(ts.Field1PK)
	require.NoError(t, err)
	storeValue, err := serializer.Serialize(ts)
	require.NoError(t, err)
	field4IdxKeyKey, err := serializer.Serialize(ts.Field4IdxKey)
	require.NoError(t, err)
	field5IdxKeyKey, err := serializer.Serialize(ts.Field5IdxKey)
	require.NoError(t, err)

	t.Run(
		"happy path",
		func(t *testing.T) {
			logger.Debugf("writing value")

			// create with indexes
			{
				err = op.
					Operate(dbMemoryInfo).
					Create(
						ctx,
						&badgerdb_operation_models_v4.Item{
							Key:   storeKey,
							Value: storeValue,
						},
						&badgerdb_operation_models_v4.IndexOpts{
							HasIdx:          true,
							ParentKey:       storeKey,
							IndexingKeys:    [][]byte{field4IdxKeyKey, field5IdxKeyKey},
							IndexProperties: badgerdb_operation_models_v4.IndexProperties{},
						},
						list_operator.DefaultRetrialOps,
					)
				require.NoError(t, err)
				logger.Debugf("value written")
			}

			t.Run(
				"errors items already created",
				func(t *testing.T) {
					logger.Debugf("writing value")

					// fail for main key
					{
						err = op.
							Operate(dbMemoryInfo).
							Create(
								ctx,
								&badgerdb_operation_models_v4.Item{
									Key:   storeKey,
									Value: storeValue,
								},
								&badgerdb_operation_models_v4.IndexOpts{
									HasIdx:          true,
									ParentKey:       storeKey,
									IndexingKeys:    [][]byte{field4IdxKeyKey, field5IdxKeyKey},
									IndexProperties: badgerdb_operation_models_v4.IndexProperties{},
								},
								list_operator.DefaultRetrialOps,
							)
						require.Error(t, err)
						logger.Debugf("value written")
					}

					newStoreKey, err := serializer.Serialize("new-main-key")
					require.NoError(t, err)

					// fail for index
					{
						err = op.
							Operate(dbMemoryInfo).
							Create(
								ctx,
								&badgerdb_operation_models_v4.Item{
									Key:   newStoreKey,
									Value: storeValue,
								},
								&badgerdb_operation_models_v4.IndexOpts{
									HasIdx:          true,
									ParentKey:       newStoreKey,
									IndexingKeys:    [][]byte{field4IdxKeyKey, field5IdxKeyKey},
									IndexProperties: badgerdb_operation_models_v4.IndexProperties{},
								},
								list_operator.DefaultRetrialOps,
							)
						require.Error(t, err)
						logger.Debugf("value written")
					}
				},
			)

			// load value
			{
				logger.Debugf("loading value")
				retrievedValue, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
				)
				require.NoError(t, err)
				logger.Debugf("value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
			}

			// load from index - one | straight search
			{
				logger.Debugf("loading value from index")
				retrievedValueFromIdx, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    nil,
						IndexingKeys: [][]byte{field4IdxKeyKey},
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexSearchPattern: badgerdb_operation_models_v4.One,
						},
					},
				)
				require.NoError(t, err)
				logger.Debugf("value loaded")
				t.Log("retrieved value ->", string(retrievedValueFromIdx))
			}

			// load from index - or computational
			{
				logger.Debugf("loading value from index - or computational")
				retrievedValueFromIdxOr, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    nil,
						IndexingKeys: [][]byte{field4IdxKeyKey, field5IdxKeyKey},
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexSearchPattern: badgerdb_operation_models_v4.OrComputational,
						},
					},
				)
				require.NoError(t, err)
				logger.Debugf("value loaded")
				t.Log("retrieved value ->", string(retrievedValueFromIdxOr))
			}

			// load from index - and computational
			{
				logger.Debugf("loading value from index - and computational")
				retrievedValueFromIdxAnd, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    nil,
						IndexingKeys: [][]byte{field4IdxKeyKey, field5IdxKeyKey},
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexSearchPattern: badgerdb_operation_models_v4.AndComputational,
						},
					},
				)
				require.NoError(t, err)
				logger.Debugf("value loaded")
				t.Log("retrieved value ->", string(retrievedValueFromIdxAnd))
			}

			// load from index - index only
			{
				logger.Debugf("loading value from index - and computational")
				retrievedValue, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{},
					&badgerdb_operation_models_v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    nil,
						IndexingKeys: [][]byte{field5IdxKeyKey},
						IndexProperties: badgerdb_operation_models_v4.IndexProperties{
							IndexSearchPattern: badgerdb_operation_models_v4.IndexingList,
						},
					},
				)
				require.NoError(t, err)
				logger.Debugf("value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
			}

			// Delete - cascade
			{
				logger.Debugf("deleting value")
				err = op.
					Operate(dbMemoryInfo).
					Delete(
						ctx,
						&badgerdb_operation_models_v4.Item{
							Key:   storeKey,
							Value: storeValue,
						},
						&badgerdb_operation_models_v4.IndexOpts{
							HasIdx: true,
							IndexProperties: badgerdb_operation_models_v4.IndexProperties{
								IndexDeletionBehaviour: badgerdb_operation_models_v4.Cascade,
							},
						},
						chainded_operator.DefaultRetrialOps,
					)
				require.NoError(t, err)
				logger.Debugf("value deleted")
			}

			// load to check
			{
				logger.Debugf("loading value")
				_, err = op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
				)
				require.Error(t, err)
				logger.Debugf("value loaded")
			}
		},
	)

	// delete store
	{
		logger.Debugf("deleting store")
		err = m.DeleteStore(ctx, dbInfoOpTest.Name)
		require.NoError(t, err)
		logger.Debugf("store deleted")
	}

	logger.Debugf("shutting down stores")
	m.ShutdownStores()
	m.Shutdown()
	logger.Debugf("stores shut down")

	logger.Infof("cleaned up all successfully")
}
