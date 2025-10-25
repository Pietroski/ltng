package v4

import (
	"bytes"
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gitlab.com/pietroski-software-company/golang/devex/random"
	"gitlab.com/pietroski-software-company/golang/devex/serializer"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"

	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
	badgerdb_operation_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/operation"
	list_operator "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/list-operator"
)

var debugMode = false

func Test_Integration_Create_Load_Delete(t *testing.T) {
	err := exec.Command("rm", "-rf", ".db").Run()
	require.NoError(t, err)

	t.Run(
		"happy path",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := slogx.New() // logger := slogx.New(slogx.WithLogLevel(slogx.LevelTest))

			logger.Test(ctx, "opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
			require.NoError(t, err)

			m, err := NewBadgerLocalManagerV4(ctx, WithDB(db))
			require.NoError(t, err)

			op, err := NewBadgerOperatorV4(ctx, WithManager(m))
			require.NoError(t, err)

			logger.Test(ctx, "starting manager")
			err = m.Start()
			require.NoError(t, err)
			logger.Test(ctx, "manager started")

			logger.Test(ctx, "creating store")
			dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Test(ctx, "store created")

			dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			logger.Test(ctx, "writing value")
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
			logger.Test(ctx, "value written")

			logger.Test(ctx, "loading value")
			retrievedValue, err := op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Test(ctx, "value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Test(ctx, "deleting value")
			err = op.
				Operate(dbMemoryInfo).
				Delete(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
					list_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Test(ctx, "value deleted")

			logger.Test(ctx, "loading value")
			retrievedValue, err = op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Test(ctx, "value loaded")

			logger.Test(ctx, "deleting store")
			err = m.DeleteStore(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)
			logger.Test(ctx, "store deleted")

			logger.Test(ctx, "shutting down stores")
			m.ShutdownStores()
			m.Shutdown()
			logger.Test(ctx, "stores shut down")

			logger.Test(ctx, "cleaned up all successfully")
		},
	)

	t.Run(
		"happy path - call with upsert",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := slogx.New() // logger := slogx.New(slogx.WithLogLevel(slogx.LevelTest))

			logger.Test(ctx, "opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
			require.NoError(t, err)

			m, err := NewBadgerLocalManagerV4(ctx, WithDB(db))
			require.NoError(t, err)

			op, err := NewBadgerOperatorV4(ctx, WithManager(m))
			require.NoError(t, err)

			logger.Test(ctx, "starting manager")
			err = m.Start()
			require.NoError(t, err)
			logger.Test(ctx, "manager started")

			logger.Test(ctx, "creating store")
			dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Test(ctx, "store created")

			dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			logger.Test(ctx, "writing value")
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
			logger.Test(ctx, "value written")

			logger.Test(ctx, "re-writing value")
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
			logger.Test(ctx, "value re-written")

			logger.Test(ctx, "loading value")
			retrievedValue, err := op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Test(ctx, "value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Test(ctx, "deleting value")
			err = op.
				Operate(dbMemoryInfo).
				Delete(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
					list_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Test(ctx, "value deleted")

			logger.Test(ctx, "loading value")
			retrievedValue, err = op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Test(ctx, "value loaded")

			logger.Test(ctx, "deleting store")
			err = m.DeleteStore(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)
			logger.Test(ctx, "store deleted")

			logger.Test(ctx, "shutting down stores")
			m.ShutdownStores()
			m.Shutdown()
			logger.Test(ctx, "stores shut down")

			logger.Test(ctx, "cleaned up all successfully")
		},
	)

	t.Run(
		"happy path - delete cascade",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := slogx.New() // logger := slogx.New(slogx.WithLogLevel(slogx.LevelTest))
			s := serializer.NewJsonSerializer()

			logger.Test(ctx, "opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
			require.NoError(t, err)

			m, err := NewBadgerLocalManagerV4(ctx, WithDB(db))
			require.NoError(t, err)

			op, err := NewBadgerOperatorV4(ctx, WithManager(m))
			require.NoError(t, err)

			logger.Test(ctx, "starting manager")
			err = m.Start()
			require.NoError(t, err)
			logger.Test(ctx, "manager started")

			logger.Test(ctx, "creating store")
			dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Test(ctx, "store created")

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
				Name:     random.String(8),
				Email:    random.Email(),
				Username: random.String(8),
			}

			key := newUUID
			value := person
			emailKey, err := s.Serialize(person.Email)
			require.NoError(t, err)
			usernameKey, err := s.Serialize(person.Username)
			require.NoError(t, err)
			storeKey, err := s.Serialize(key)
			require.NoError(t, err)
			storeValue, err := s.Serialize(value)
			require.NoError(t, err)
			logger.Test(ctx, "writing value")
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
			logger.Test(ctx, "value written")

			logger.Test(ctx, "loading value")
			retrievedValue, err := op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Test(ctx, "value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Test(ctx, "loading value for indexed list")
			indexedListName := dbMemoryInfo.Name + IndexedListSuffixName
			idxListMemoryInfo, err := m.GetDBMemoryInfo(ctx, indexedListName)
			retrievedValue, err = op.Operate(idxListMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Test(ctx, "value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Test(ctx, "deleting value")
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
					list_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Test(ctx, "value deleted")

			logger.Test(ctx, "loading value")
			retrievedValue, err = op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Test(ctx, "value loaded")

			logger.Test(ctx, "loading value for indexed list")
			indexedListName = dbMemoryInfo.Name + IndexedListSuffixName
			idxListMemoryInfo, err = m.GetDBMemoryInfo(ctx, indexedListName)
			retrievedValue, err = op.Operate(idxListMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Test(ctx, "value loaded")

			logger.Test(ctx, "deleting store")
			err = m.DeleteStore(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)
			logger.Test(ctx, "store deleted")

			logger.Test(ctx, "shutting down stores")
			m.ShutdownStores()
			m.Shutdown()
			logger.Test(ctx, "stores shut down")

			logger.Test(ctx, "cleaned up all successfully")
		},
	)

	t.Run(
		"happy path - delete cascade by index",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := slogx.New() // logger := slogx.New(slogx.WithLogLevel(slogx.LevelTest))
			s := serializer.NewJsonSerializer()

			logger.Test(ctx, "opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
			require.NoError(t, err)

			m, err := NewBadgerLocalManagerV4(ctx, WithDB(db))
			require.NoError(t, err)

			op, err := NewBadgerOperatorV4(ctx, WithManager(m))
			require.NoError(t, err)

			logger.Test(ctx, "starting manager")
			err = m.Start()
			require.NoError(t, err)
			logger.Test(ctx, "manager started")

			logger.Test(ctx, "creating store")
			dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Test(ctx, "store created")

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
				Name:     random.String(8),
				Email:    random.Email(),
				Username: random.String(8),
			}

			key := newUUID
			value := person
			emailKey, err := s.Serialize(person.Email)
			require.NoError(t, err)
			usernameKey, err := s.Serialize(person.Username)
			require.NoError(t, err)
			storeKey, err := s.Serialize(key)
			require.NoError(t, err)
			storeValue, err := s.Serialize(value)
			require.NoError(t, err)
			logger.Test(ctx, "writing value")
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
			logger.Test(ctx, "value written")

			logger.Test(ctx, "loading value")
			retrievedValue, err := op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Test(ctx, "value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Test(ctx, "loading value for indexed list")
			indexedListName := dbMemoryInfo.Name + IndexedListSuffixName
			idxListMemoryInfo, err := m.GetDBMemoryInfo(ctx, indexedListName)
			retrievedValue, err = op.Operate(idxListMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Test(ctx, "value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Test(ctx, "deleting value")
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
					list_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Test(ctx, "value deleted")

			logger.Test(ctx, "loading value")
			retrievedValue, err = op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Test(ctx, "value loaded")

			logger.Test(ctx, "loading value for indexed list")
			indexedListName = dbMemoryInfo.Name + IndexedListSuffixName
			idxListMemoryInfo, err = m.GetDBMemoryInfo(ctx, indexedListName)
			retrievedValue, err = op.Operate(idxListMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Test(ctx, "value loaded")

			logger.Test(ctx, "deleting store")
			err = m.DeleteStore(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)
			logger.Test(ctx, "store deleted")

			logger.Test(ctx, "shutting down stores")
			m.ShutdownStores()
			m.Shutdown()
			logger.Test(ctx, "stores shut down")

			logger.Test(ctx, "cleaned up all successfully")
		},
	)

	t.Run(
		"happy path - delete cascade - call with upsert",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := slogx.New() // logger := slogx.New(slogx.WithLogLevel(slogx.LevelTest))
			s := serializer.NewJsonSerializer()

			logger.Test(ctx, "opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
			require.NoError(t, err)

			m, err := NewBadgerLocalManagerV4(ctx, WithDB(db))
			require.NoError(t, err)

			op, err := NewBadgerOperatorV4(ctx, WithManager(m))
			require.NoError(t, err)

			logger.Test(ctx, "starting manager")
			err = m.Start()
			require.NoError(t, err)
			logger.Test(ctx, "manager started")

			logger.Test(ctx, "creating store")
			dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Test(ctx, "store created")

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
				Name:     random.String(8),
				Email:    random.Email(),
				Username: random.String(8),
			}

			key := newUUID
			value := person
			emailKey, err := s.Serialize(person.Email)
			require.NoError(t, err)
			usernameKey, err := s.Serialize(person.Username)
			require.NoError(t, err)
			storeKey, err := s.Serialize(key)
			require.NoError(t, err)
			storeValue, err := s.Serialize(value)
			require.NoError(t, err)
			logger.Test(ctx, "writing value")
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
			logger.Test(ctx, "value written")

			logger.Test(ctx, "loading value")
			retrievedValue, err := op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Test(ctx, "value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Test(ctx, "loading value for indexed list")
			indexedListName := dbMemoryInfo.Name + IndexedListSuffixName
			idxListMemoryInfo, err := m.GetDBMemoryInfo(ctx, indexedListName)
			retrievedValue, err = op.Operate(idxListMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Test(ctx, "value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Test(ctx, "deleting value")
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
					list_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Test(ctx, "value deleted")

			logger.Test(ctx, "loading value")
			retrievedValue, err = op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Test(ctx, "value loaded")

			logger.Test(ctx, "loading value for indexed list")
			indexedListName = dbMemoryInfo.Name + IndexedListSuffixName
			idxListMemoryInfo, err = m.GetDBMemoryInfo(ctx, indexedListName)
			retrievedValue, err = op.Operate(idxListMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key: storeKey,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Test(ctx, "value loaded")

			logger.Test(ctx, "deleting store")
			err = m.DeleteStore(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)
			logger.Test(ctx, "store deleted")

			logger.Test(ctx, "shutting down stores")
			m.ShutdownStores()
			m.Shutdown()
			logger.Test(ctx, "stores shut down")

			logger.Test(ctx, "cleaned up all successfully")
		},
	)

	t.Run(
		"happy path - delete index only - call with upsert",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := slogx.New() // logger := slogx.New(slogx.WithLogLevel(slogx.LevelTest))
			s := serializer.NewJsonSerializer()

			logger.Test(ctx, "opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
			require.NoError(t, err)

			m, err := NewBadgerLocalManagerV4(ctx, WithDB(db))
			require.NoError(t, err)

			op, err := NewBadgerOperatorV4(ctx, WithManager(m))
			require.NoError(t, err)

			logger.Test(ctx, "starting manager")
			err = m.Start()
			require.NoError(t, err)
			logger.Test(ctx, "manager started")

			logger.Test(ctx, "creating store")
			dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Test(ctx, "store created")

			storeList, err := m.ListStoreMemoryInfo(ctx, 0, 0)
			require.NoError(t, err)
			logger.Test(ctx, "stores list result", "stores", storeList)

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
				Name:     random.String(8),
				Email:    random.Email(),
				Username: random.String(8),
			}

			key := newUUID
			value := person
			emailKey, err := s.Serialize(person.Email)
			require.NoError(t, err)
			usernameKey, err := s.Serialize(person.Username)
			require.NoError(t, err)
			storeKey, err := s.Serialize(key)
			require.NoError(t, err)
			storeValue, err := s.Serialize(value)
			require.NoError(t, err)
			// UPSERT
			{
				logger.Test(ctx, "writing value")
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
				logger.Test(ctx, "value written")
			}

			// value from main key
			{
				logger.Test(ctx, "loading value")
				retrievedValue, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key: storeKey,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
				)
				require.NoError(t, err)
				logger.Test(ctx, "value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				require.Equal(t, storeValue, retrievedValue)
			}

			// indexed list
			{
				logger.Test(ctx, "loading value for indexed list")
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
				logger.Test(ctx, "value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				result := bytes.Join([][]byte{emailKey, usernameKey}, []byte(bsSeparator))
				require.Equal(t, result, retrievedValue)
			}

			// indexed list
			{
				logger.Test(ctx, "loading value for indexed list")
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
				logger.Test(ctx, "value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				result := bytes.Join([][]byte{emailKey, usernameKey}, []byte(bsSeparator))
				require.Equal(t, result, retrievedValue)
			}

			// value from index item - AndComputational
			{
				logger.Test(ctx, "loading value from index item")
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
				logger.Test(ctx, "value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				require.Equal(t, storeValue, retrievedValue)
			}

			// value from index item - OrComputational
			{
				logger.Test(ctx, "loading value from index item")
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
				logger.Test(ctx, "value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				require.Equal(t, storeValue, retrievedValue)
			}

			// value from index item - and computational - two items
			{
				logger.Test(ctx, "loading value for index item")
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
				logger.Test(ctx, "value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				require.Equal(t, storeValue, retrievedValue)
			}

			// value from index item - or computational - two items
			{
				logger.Test(ctx, "loading value for index item")
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
				logger.Test(ctx, "value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				require.Equal(t, storeValue, retrievedValue)
			}

			// DELETE - single key
			{
				logger.Test(ctx, "deleting value")
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
						list_operator.DefaultRetrialOps,
					)
				require.NoError(t, err)
				logger.Test(ctx, "value deleted")
			}

			// value from main key
			{
				logger.Test(ctx, "loading value")
				retrievedValue, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key: storeKey,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
				)
				require.NoError(t, err)
				logger.Test(ctx, "value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				require.Equal(t, storeValue, retrievedValue)
			}

			// indexed list
			{
				logger.Test(ctx, "loading value for indexed list")
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
				logger.Test(ctx, "value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				result := bytes.Join([][]byte{emailKey}, []byte(bsSeparator))
				require.Equal(t, result, retrievedValue)
			}

			// value from index item - AndComputational
			{
				logger.Test(ctx, "loading value from index item")
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
				logger.Test(ctx, "value loaded")
				require.Equal(t, []byte{}, retrievedValue)
			}

			// value from index item - One
			{
				logger.Test(ctx, "loading value from indexed item")
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
				logger.Test(ctx, "value loaded")
			}

			// value from index item - One
			{
				logger.Test(ctx, "loading value for indexed item")
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
				logger.Test(ctx, "value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
				require.Equal(t, storeValue, retrievedValue)
			}

			// DELETE - cascade
			{
				logger.Test(ctx, "deleting value")
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
						list_operator.DefaultRetrialOps,
					)
				require.NoError(t, err)
				logger.Test(ctx, "value deleted")
			}

			// value from main key
			{
				logger.Test(ctx, "loading value")
				_, err = op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key: storeKey,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
				)
				require.Error(t, err)
				logger.Test(ctx, "value loaded")
			}

			// indexed list
			{
				logger.Test(ctx, "loading value for indexed list")
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
				logger.Test(ctx, "value loaded")
				require.Equal(t, []byte(nil), retrievedValue)
			}

			logger.Test(ctx, "deleting store")
			err = m.DeleteStore(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)
			logger.Test(ctx, "store deleted")

			logger.Test(ctx, "shutting down stores")
			m.ShutdownStores()
			m.Shutdown()
			logger.Test(ctx, "stores shut down")

			logger.Test(ctx, "cleaned up all successfully")
		},
	)

	t.Run(
		"item already created",
		func(t *testing.T) {
			ctx, _ := context.WithCancel(context.Background())
			logger := slogx.New() // logger := slogx.New(slogx.WithLogLevel(slogx.LevelTest))

			logger.Test(ctx, "opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
			require.NoError(t, err)

			m, err := NewBadgerLocalManagerV4(ctx, WithDB(db))
			require.NoError(t, err)

			op, err := NewBadgerOperatorV4(ctx, WithManager(m))
			require.NoError(t, err)

			logger.Test(ctx, "starting manager")
			err = m.Start()
			require.NoError(t, err)
			logger.Test(ctx, "manager started")

			logger.Test(ctx, "creating store")
			dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Test(ctx, "store created")

			dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)

			storeKey := []byte("key-test-to-store")
			storeValue := []byte("value test to store")
			logger.Test(ctx, "writing value")
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
			logger.Test(ctx, "value written")

			logger.Test(ctx, "writing value")
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
			logger.Test(ctx, "value written")

			logger.Test(ctx, "loading value")
			retrievedValue, err := op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.NoError(t, err)
			logger.Test(ctx, "value loaded")
			t.Log("retrieved value ->", string(retrievedValue))

			logger.Test(ctx, "deleting value")
			err = op.
				Operate(dbMemoryInfo).
				Delete(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
					list_operator.DefaultRetrialOps,
				)
			require.NoError(t, err)
			logger.Test(ctx, "value deleted")

			logger.Test(ctx, "loading value")
			retrievedValue, err = op.Operate(dbMemoryInfo).Load(
				ctx,
				&badgerdb_operation_models_v4.Item{
					Key:   storeKey,
					Value: storeValue,
				},
				&badgerdb_operation_models_v4.IndexOpts{},
			)
			require.Error(t, err)
			logger.Test(ctx, "value loaded")

			logger.Test(ctx, "deleting store")
			err = m.DeleteStore(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)
			logger.Test(ctx, "store deleted")

			logger.Test(ctx, "shutting down stores")
			m.ShutdownStores()
			m.Shutdown()
			logger.Test(ctx, "stores shut down")

			logger.Test(ctx, "cleaned up all successfully")
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
			logger := slogx.New() // logger := slogx.New(slogx.WithLogLevel(slogx.LevelTest))
			s := serializer.NewJsonSerializer()

			logger.Test(ctx, "opening badger local manager")
			db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
			require.NoError(t, err)

			m, err := NewBadgerLocalManagerV4(ctx, WithDB(db))
			require.NoError(t, err)

			op, err := NewBadgerOperatorV4(ctx, WithManager(m))
			require.NoError(t, err)

			logger.Test(ctx, "starting manager")
			err = m.Start()
			require.NoError(t, err)
			logger.Test(ctx, "manager started")

			logger.Test(ctx, "creating store")
			dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
				Name:         "operations-db-integration-test",
				Path:         "operations-db-integration-test",
				CreatedAt:    time.Now(),
				LastOpenedAt: time.Now(),
			}
			err = m.CreateStore(ctx, dbInfoOpTest)
			require.NoError(t, err)
			logger.Test(ctx, "store created")

			storeList, err := m.ListStoreMemoryInfo(ctx, 0, 0)
			require.NoError(t, err)
			logger.Test(ctx, "stores list result", "stores", storeList)

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
					Name:     random.String(8),
					Email:    random.Email(),
					Username: random.String(8),
				}

				people[idx] = person
			}

			{
				for _, person := range people {
					key := person.ID
					value := person

					emailKey, err := s.Serialize(person.Email)
					require.NoError(t, err)
					usernameKey, err := s.Serialize(person.Username)
					require.NoError(t, err)
					storeKey, err := s.Serialize(key)
					require.NoError(t, err)
					storeValue, err := s.Serialize(value)
					require.NoError(t, err)

					// UPSERT
					{
						logger.Test(ctx, "writing value")
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
						logger.Test(ctx, "value written")
					}
				}

				// list default
				{
					logger.Test(ctx, "listing default pagination values")
					defaultPaginatedRetrievedValues, err := op.Operate(dbMemoryInfo).
						List(
							ctx,
							//&badgerdb_operation_models_v4.Item{},
							&badgerdb_operation_models_v4.IndexOpts{},
							&badgerdb_management_models_v4.Pagination{},
						)
					require.NoError(t, err)
					logger.Test(ctx, "values loaded")
					t.Log("retrieved value ->", defaultPaginatedRetrievedValues)
				}

				// list all
				{
					logger.Test(ctx, "listing all values")
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
					logger.Test(ctx, "values loaded")
					t.Log("retrieved value ->", retrievedValues)
				}

				// list paginated
				{
					logger.Test(ctx, "listing values paginated")
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
					logger.Test(ctx, "values loaded")
					t.Log("retrieved value ->", paginatedRetrievedValues)
				}

				// list items
				{
					logger.Test(ctx, "listing list items values")
					p0, err := s.Serialize(people[0].Email)
					require.NoError(t, err)
					p1, err := s.Serialize(people[1].Email)
					require.NoError(t, err)
					p4, err := s.Serialize(people[4].Email)
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
					logger.Test(ctx, "listing list items values")
					p0, err := s.Serialize(people[0].Email)
					require.NoError(t, err)
					p1, err := s.Serialize(people[1].Email)
					require.NoError(t, err)
					p4, err := s.Serialize(people[4].Email)
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
					storeKey, err := s.Serialize(key)
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
							list_operator.DefaultRetrialOps,
						)
					}
				}

				// list all
				{
					logger.Test(ctx, "listing all values")
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
					logger.Test(ctx, "values loaded")
					t.Log("retrieved value ->", retrievedValues)
				}
			}

			logger.Test(ctx, "deleting store")
			err = m.DeleteStore(ctx, dbInfoOpTest.Name)
			require.NoError(t, err)
			logger.Test(ctx, "store deleted")

			logger.Test(ctx, "shutting down stores")
			m.ShutdownStores()
			m.Shutdown()
			logger.Test(ctx, "stores shut down")

			logger.Test(ctx, "cleaned up all successfully")
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
		Field1PK:     random.String(7),
		Field2:       random.String(7),
		Filed3:       random.String(7),
		Field4IdxKey: random.String(7),
		Field5IdxKey: random.String(7),
	}

	ctx, _ := context.WithCancel(context.Background())
	logger := slogx.New() // logger := slogx.New(slogx.WithLogLevel(slogx.LevelTest))
	s := serializer.NewJsonSerializer()

	logger.Test(ctx, "opening badger local manager")
	db, err := badger.Open(badger.DefaultOptions(InternalLocalManagement))
	require.NoError(t, err)

	m, err := NewBadgerLocalManagerV4(ctx, WithDB(db))
	require.NoError(t, err)

	op, err := NewBadgerOperatorV4(ctx, WithManager(m))
	require.NoError(t, err)

	logger.Test(ctx, "starting manager")
	err = m.Start()
	require.NoError(t, err)
	logger.Test(ctx, "manager started")

	logger.Test(ctx, "creating store")
	dbInfoOpTest := &badgerdb_management_models_v4.DBInfo{
		Name:         "operations-db-integration-test",
		Path:         "operations-db-integration-test",
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Now(),
	}
	err = m.CreateStore(ctx, dbInfoOpTest)
	require.NoError(t, err)
	logger.Test(ctx, "store created")

	dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, dbInfoOpTest.Name)
	require.NoError(t, err)

	storeKey, err := s.Serialize(ts.Field1PK)
	require.NoError(t, err)
	storeValue, err := s.Serialize(ts)
	require.NoError(t, err)
	field4IdxKeyKey, err := s.Serialize(ts.Field4IdxKey)
	require.NoError(t, err)
	field5IdxKeyKey, err := s.Serialize(ts.Field5IdxKey)
	require.NoError(t, err)

	t.Run(
		"happy path",
		func(t *testing.T) {
			logger.Test(ctx, "writing value")

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
				logger.Test(ctx, "value written")
			}

			t.Run(
				"errors items already created",
				func(t *testing.T) {
					logger.Test(ctx, "writing value")

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
						logger.Test(ctx, "value written")
					}

					newStoreKey, err := s.Serialize("new-main-key")
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
						logger.Test(ctx, "value written")
					}
				},
			)

			// load value
			{
				logger.Test(ctx, "loading value")
				retrievedValue, err := op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
				)
				require.NoError(t, err)
				logger.Test(ctx, "value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
			}

			// load from index - one | straight search
			{
				logger.Test(ctx, "loading value from index")
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
				logger.Test(ctx, "value loaded")
				t.Log("retrieved value ->", string(retrievedValueFromIdx))
			}

			// load from index - or computational
			{
				logger.Test(ctx, "loading value from index - or computational")
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
				logger.Test(ctx, "value loaded")
				t.Log("retrieved value ->", string(retrievedValueFromIdxOr))
			}

			// load from index - and computational
			{
				logger.Test(ctx, "loading value from index - and computational")
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
				logger.Test(ctx, "value loaded")
				t.Log("retrieved value ->", string(retrievedValueFromIdxAnd))
			}

			// load from index - index only
			{
				logger.Test(ctx, "loading value from index - and computational")
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
				logger.Test(ctx, "value loaded")
				t.Log("retrieved value ->", string(retrievedValue))
			}

			// Delete - cascade
			{
				logger.Test(ctx, "deleting value")
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
						list_operator.DefaultRetrialOps,
					)
				require.NoError(t, err)
				logger.Test(ctx, "value deleted")
			}

			// load to check
			{
				logger.Test(ctx, "loading value")
				_, err = op.Operate(dbMemoryInfo).Load(
					ctx,
					&badgerdb_operation_models_v4.Item{
						Key:   storeKey,
						Value: storeValue,
					},
					&badgerdb_operation_models_v4.IndexOpts{},
				)
				require.Error(t, err)
				logger.Test(ctx, "value loaded")
			}
		},
	)

	// delete store
	{
		logger.Test(ctx, "deleting store")
		err = m.DeleteStore(ctx, dbInfoOpTest.Name)
		require.NoError(t, err)
		logger.Test(ctx, "store deleted")
	}

	logger.Test(ctx, "shutting down stores")
	m.ShutdownStores()
	m.Shutdown()
	logger.Test(ctx, "stores shut down")

	logger.Test(ctx, "cleaned up all successfully")
}
