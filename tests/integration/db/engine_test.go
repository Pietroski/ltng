package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.com/pietroski-software-company/golang/devex/execx"
	"gitlab.com/pietroski-software-company/golang/devex/saga"

	filequeuev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/file_queue/v1"
	models_badgerdb_v4_management "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
	models_badgerdb_v4_operation "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/operation"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/testbench"
	"gitlab.com/pietroski-software-company/lightning-db/tests/data"
)

var (
	ets *data.EngineTestSuite
)

func TestEngines(t *testing.T) {
	users = data.GenerateRandomUsers(t, 100)
	ets = data.InitEngineTestSuite(t)

	t.Log("testLTNGDBEngineV2")
	testLTNGDBEngineV2(t)

	t.Log("testBadgerDBEngine")
	testBadgerDBEngine(t)
}

func testLTNGDBEngineV2(t *testing.T) {
	storeInfo := &ltngdata.StoreInfo{
		Name: "user-store",
		Path: "user-store",
	}
	dbMetaInfo := storeInfo.ManagerStoreMetaInfo()
	_, err := ets.LTNGDBEngineV2.CreateStore(ets.Ctx, storeInfo)
	require.NoError(t, err)

	store, err := ets.LTNGDBEngineV2.LoadStore(ets.Ctx, storeInfo)
	require.NoError(t, err)
	require.NotNil(t, store)

	{
		t.Log("CreateItem")

		bd := testbench.New()
		for _, user := range users {
			bvs := data.GetUserBytesValues(t, ets.TS(), user)

			item := &ltngdata.Item{
				Key:   bvs.BsKey,
				Value: bvs.BsValue,
			}
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				ParentKey:    bvs.BsKey,
				IndexingKeys: [][]byte{bvs.BsKey, bvs.SecondaryIndexBs},
			}
			bd.CalcAvg(bd.CalcElapsed(func() {
				_, err = ets.LTNGDBEngineV2.CreateItem(ets.Ctx, dbMetaInfo, item, opts)
			}))
			assert.NoError(t, err)
		}
		t.Log(bd)
	}

	{
		t.Log("ListItems")

		bd := testbench.New()
		pagination := &ltngdata.Pagination{
			PageID:   1,
			PageSize: 50,
		}
		opts := &ltngdata.IndexOpts{
			IndexProperties: ltngdata.IndexProperties{
				ListSearchPattern: ltngdata.Default,
			},
		}
		bd.CalcAvg(bd.CalcElapsed(func() {
			_, err = ets.LTNGDBEngineV2.ListItems(ets.Ctx, dbMetaInfo, pagination, opts)
		}))
		assert.NoError(t, err)
		t.Log(bd)
	}

	{
		t.Log("UpsertItem")

		bd := testbench.New()
		for _, user := range users {
			bvs := data.GetUserBytesValues(t, ets.TS(), user)

			item := &ltngdata.Item{
				Key:   bvs.BsKey,
				Value: bvs.BsValue,
			}
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				ParentKey:    bvs.BsKey,
				IndexingKeys: [][]byte{bvs.BsKey, bvs.SecondaryIndexBs},
			}
			bd.CalcAvg(bd.CalcElapsed(func() {
				_, err = ets.LTNGDBEngineV2.UpsertItem(ets.Ctx, dbMetaInfo, item, opts)
			}))
			assert.NoError(t, err)
		}
		t.Log(bd)
	}

	{
		t.Log("DeleteItem")

		bd := testbench.New()
		for _, user := range users {
			bvs := data.GetUserBytesValues(t, ets.TS(), user)

			item := &ltngdata.Item{
				Key:   bvs.BsKey,
				Value: bvs.BsValue,
			}
			opts := &ltngdata.IndexOpts{
				HasIdx:    true,
				ParentKey: bvs.BsKey,
				IndexProperties: ltngdata.IndexProperties{
					IndexDeletionBehaviour: ltngdata.Cascade,
				},
			}
			bd.CalcAvg(bd.CalcElapsed(func() {
				_, err = ets.LTNGDBEngineV2.DeleteItem(ets.Ctx, dbMetaInfo, item, opts)
			}))
			assert.NoError(t, err)
		}
		t.Log(bd)
	}

	ets.LTNGDBEngineV2.Close()
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

		bd := testbench.New()
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
			bd.CalcAvg(bd.CalcElapsed(func() {
				err = ets.BadgerDBEngine.Operator.Operate(dbMemoryInfo).
					Create(ets.Ctx, item, opts, saga.DefaultRetrialOps)
			}))
			assert.NoError(t, err)
		}
		t.Log(bd)
	}

	{
		t.Log("ListItems")

		bd := testbench.New()
		opts := &models_badgerdb_v4_operation.IndexOpts{
			IndexProperties: models_badgerdb_v4_operation.IndexProperties{
				ListSearchPattern: models_badgerdb_v4_operation.Default,
			},
		}
		pagination := &models_badgerdb_v4_management.Pagination{
			PageID:   1,
			PageSize: 10,
		}
		bd.CalcAvg(bd.CalcElapsed(func() {
			_, err = ets.BadgerDBEngine.Operator.Operate(dbMemoryInfo).List(ets.Ctx, opts, pagination)
		}))
		assert.NoError(t, err)
		t.Log(bd)
	}

	{
		t.Log("UpsertItem")

		bd := testbench.New()
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
			bd.CalcAvg(bd.CalcElapsed(func() {
				err = ets.BadgerDBEngine.Operator.Operate(dbMemoryInfo).
					Upsert(ets.Ctx, item, opts, saga.DefaultRetrialOps)
			}))
			assert.NoError(t, err)
		}
		t.Log(bd)
	}

	{
		t.Log("DeleteItem")

		bd := testbench.New()
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
			bd.CalcAvg(bd.CalcElapsed(func() {
				err = ets.BadgerDBEngine.Operator.Operate(dbMemoryInfo).
					Upsert(ets.Ctx, item, opts, saga.DefaultRetrialOps)
			}))
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
	ctx := context.Background()
	fq, err := filequeuev1.New(ctx,
		filequeuev1.GenericFileQueueFilePath, filequeuev1.GenericFileQueueFileName)
	require.NoError(t, err)

	var counter int
	for {
		_, err = fq.Read(ctx)
		if err != nil {
			t.Log(err)
			break
		}

		err = fq.Pop(ctx)
		assert.NoError(t, err)

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
