package db_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.com/pietroski-software-company/golang/devex/execx"
	"gitlab.com/pietroski-software-company/golang/devex/saga"
	"gitlab.com/pietroski-software-company/golang/devex/testingx"

	models_badgerdb_v4_management "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
	models_badgerdb_v4_operation "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/operation"
	ltngdbmodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/mmap"
	fileiomodels "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/models"
	"gitlab.com/pietroski-software-company/lightning-db/tests/data"
)

var (
	ets *data.EngineTestSuite
)

func TestEngines(t *testing.T) {
	users = data.GenerateRandomUsers(t, 100)
	ets = data.InitEngineTestSuite(t)

	t.Log("testLTNGDBEngineV3")
	testLTNGDBEngineV3(t)

	t.Log("testBadgerDBEngine")
	testBadgerDBEngine(t)
}

func testLTNGDBEngineV3(t *testing.T) {
	storeInfo := &ltngdbmodelsv3.StoreInfo{
		Name: "user-store",
		Path: "user-store",
	}
	dbMetaInfo := storeInfo.ManagerStoreMetaInfo()
	_, err := ets.LTNGDBEngineV3.CreateStore(ets.Ctx, storeInfo)
	require.NoError(t, err)

	store, err := ets.LTNGDBEngineV3.LoadStore(ets.Ctx, storeInfo)
	require.NoError(t, err)
	require.NotNil(t, store)

	{
		t.Log("CreateItem")

		bd := testingx.NewBenchSync()
		for _, user := range users {
			bvs := data.GetUserBytesValues(t, ets.TS(), user)

			item := &ltngdbmodelsv3.Item{
				Key:   bvs.BsKey,
				Value: bvs.BsValue,
			}
			opts := &ltngdbmodelsv3.IndexOpts{
				HasIdx:       true,
				ParentKey:    bvs.BsKey,
				IndexingKeys: [][]byte{bvs.BsKey, bvs.SecondaryIndexBs},
			}
			bd.CalcElapsedAvg(func() {
				_, err = ets.LTNGDBEngineV3.CreateItem(ets.Ctx, dbMetaInfo, item, opts)
			})
			assert.NoError(t, err)
		}
		t.Log(bd)
	}

	{
		t.Log("ListItems")

		bd := testingx.NewBenchSync()
		pagination := &ltngdata.Pagination{
			PageID:   1,
			PageSize: 50,
		}
		opts := &ltngdbmodelsv3.IndexOpts{
			IndexProperties: ltngdbmodelsv3.IndexProperties{
				ListSearchPattern: ltngdbmodelsv3.Default,
			},
		}
		bd.CalcElapsedAvg(func() {
			_, err = ets.LTNGDBEngineV3.ListItems(ets.Ctx, dbMetaInfo, pagination, opts)
		})
		assert.NoError(t, err)
		t.Log(bd)
	}

	{
		t.Log("UpsertItem")

		bd := testingx.NewBenchSync()
		for _, user := range users {
			bvs := data.GetUserBytesValues(t, ets.TS(), user)

			item := &ltngdbmodelsv3.Item{
				Key:   bvs.BsKey,
				Value: bvs.BsValue,
			}
			opts := &ltngdbmodelsv3.IndexOpts{
				HasIdx:       true,
				ParentKey:    bvs.BsKey,
				IndexingKeys: [][]byte{bvs.BsKey, bvs.SecondaryIndexBs},
			}
			bd.CalcElapsedAvg(func() {
				_, err = ets.LTNGDBEngineV3.UpsertItem(ets.Ctx, dbMetaInfo, item, opts)
			})
			assert.NoError(t, err)
		}
		t.Log(bd)
	}

	{
		t.Log("DeleteItem")

		bd := testingx.NewBenchSync()
		for _, user := range users {
			bvs := data.GetUserBytesValues(t, ets.TS(), user)

			item := &ltngdbmodelsv3.Item{
				Key:   bvs.BsKey,
				Value: bvs.BsValue,
			}
			opts := &ltngdbmodelsv3.IndexOpts{
				HasIdx:    true,
				ParentKey: bvs.BsKey,
				IndexProperties: ltngdbmodelsv3.IndexProperties{
					IndexDeletionBehaviour: ltngdbmodelsv3.Cascade,
				},
			}
			bd.CalcElapsedAvg(func() {
				_, err = ets.LTNGDBEngineV3.DeleteItem(ets.Ctx, dbMetaInfo, item, opts)
			})
			assert.NoError(t, err)
		}
		t.Log(bd)
	}

	ets.LTNGDBEngineV3.Close()
}

func testBadgerDBEngine(t *testing.T) {
	dbInfo := &models_badgerdb_v4_management.DBInfo{
		Name:         "user-store",
		Path:         "user-store",
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Now(),
	}

	dbMemoryInfo := dbInfo.InfoToMemoryInfo(ets.BadgerDBEngine.DB)

	err := ets.BadgerDBEngine.Manager.CreateStore(ets.Ctx, dbInfo)
	require.NoError(t, err)

	store, err := ets.BadgerDBEngine.Manager.GetDBInfo(ets.Ctx, dbInfo.Name)
	require.NoError(t, err)
	require.NotNil(t, store)

	{
		t.Log("CreateItem")

		bd := testingx.NewBenchSync()
		for _, user := range users {
			bvs := data.GetUserBytesValues(t, ets.TS(), user)
			item := &models_badgerdb_v4_operation.Item{
				Key:   bvs.BsKey,
				Value: bvs.BsValue,
			}
			opts := &models_badgerdb_v4_operation.IndexOpts{
				HasIdx:       true,
				ParentKey:    bvs.BsKey,
				IndexingKeys: [][]byte{bvs.BsKey, bvs.SecondaryIndexBs},
			}
			bd.CalcElapsedAvg(func() {
				err = ets.BadgerDBEngine.Operator.Operate(dbMemoryInfo).
					Create(ets.Ctx, item, opts, saga.DefaultRetrialOps)
			})
			assert.NoError(t, err)
		}
		t.Log(bd)
	}

	{
		t.Log("ListItems")

		bd := testingx.NewBenchSync()
		opts := &models_badgerdb_v4_operation.IndexOpts{
			IndexProperties: models_badgerdb_v4_operation.IndexProperties{
				ListSearchPattern: models_badgerdb_v4_operation.Default,
			},
		}
		pagination := &models_badgerdb_v4_management.Pagination{
			PageID:   1,
			PageSize: 10,
		}
		bd.CalcElapsedAvg(func() {
			_, err = ets.BadgerDBEngine.Operator.Operate(dbMemoryInfo).List(ets.Ctx, opts, pagination)
		})
		assert.NoError(t, err)
		t.Log(bd)
	}

	{
		t.Log("UpsertItem")

		bd := testingx.NewBenchSync()
		for _, user := range users {
			bvs := data.GetUserBytesValues(t, ets.TS(), user)
			item := &models_badgerdb_v4_operation.Item{
				Key:   bvs.BsKey,
				Value: bvs.BsValue,
			}
			opts := &models_badgerdb_v4_operation.IndexOpts{
				HasIdx:       true,
				ParentKey:    bvs.BsKey,
				IndexingKeys: [][]byte{bvs.BsKey, bvs.SecondaryIndexBs, bvs.ExtraUpsertIndex},
			}
			bd.CalcElapsedAvg(func() {
				err = ets.BadgerDBEngine.Operator.Operate(dbMemoryInfo).
					Upsert(ets.Ctx, item, opts, saga.DefaultRetrialOps)
			})
			assert.NoError(t, err)
		}
		t.Log(bd)
	}

	{
		t.Log("DeleteItem")

		bd := testingx.NewBenchSync()
		for _, user := range users {
			bvs := data.GetUserBytesValues(t, ets.TS(), user)

			item := &models_badgerdb_v4_operation.Item{
				Key:   bvs.BsKey,
				Value: bvs.BsValue,
			}
			opts := &models_badgerdb_v4_operation.IndexOpts{
				HasIdx:    true,
				ParentKey: bvs.BsKey,
				IndexProperties: models_badgerdb_v4_operation.IndexProperties{
					IndexDeletionBehaviour: models_badgerdb_v4_operation.Cascade,
				},
			}
			bd.CalcElapsedAvg(func() {
				err = ets.BadgerDBEngine.Operator.Operate(dbMemoryInfo).
					Upsert(ets.Ctx, item, opts, saga.DefaultRetrialOps)
			})
			assert.NoError(t, err)
		}
		t.Log(bd)
	}
}

// TestReadFromFQ
// === RUN   TestReadFromFQ
// engine_test.go:293: 900
// --- PASS: TestReadFromFQ (0.23s)
// PASS
func TestReadFromFQ(t *testing.T) {
	fq, err := mmap.NewFileQueue(fileiomodels.GetFileQueueFilePath(fileiomodels.FileQueueMmapVersion,
		fileiomodels.GenericFileQueueFilePath, fileiomodels.GenericFileQueueFileName))
	require.NoError(t, err)

	var counter int
	for {
		_, err = fq.Read()
		if err != nil {
			t.Log(err)
			break
		}

		_, err = fq.Pop()
		require.NoError(t, err)

		counter++
	}
	t.Log(counter)
}

func TestCheckFileCount(t *testing.T) {
	out, err := execx.RunOutput("sh", "-c",
		"find .ltngdb/v2/stores/user-store -maxdepth 1 -type f | wc -l",
	)
	require.NoError(t, err)
	t.Log(string(out))
}
