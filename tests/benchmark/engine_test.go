package benchmark

import (
	"context"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/testbench"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/execx"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ltng_engine_v1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v1"
	ltng_engine_concurrent_v1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v1/concurrent"
	filequeuev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/file_queue/v1"
	models_badgerdb_v4_management "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
	models_badgerdb_v4_operation "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/operation"
	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	list_operator "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/list-operator"
	"gitlab.com/pietroski-software-company/lightning-db/tests/data"
)

var (
	ets *data.EngineTestSuite
)

func BenchmarkAllEngines(b *testing.B) {
	users = data.GenerateRandomUsers(b, 50)
	ets = data.InitEngineTestSuite(b)

	b.Log("Benchmark_LTNGDB_Engine")
	benchmarkLTNGDBEngine(b)

	b.Log("Benchmark_LTNGDB_Concurrent_Engine")
	benchmarkLTNGDBConcurrentEngine(b)

	b.Log("Benchmark_BadgerDB_Engine")
	benchmarkBadgerDBEngine(b)
}

func Benchmark_LTNGDB_Engine(b *testing.B) {
	users = data.GenerateRandomUsers(b, 50)
	ets = data.InitEngineTestSuite(b)
	b.Log("Benchmark_LTNGDB_Engine")
	benchmarkLTNGDBEngine(b)
}

func benchmarkLTNGDBEngine(b *testing.B) {
	storeInfo := &ltng_engine_v1.StoreInfo{
		Name: "user-store",
		Path: "user-store",
	}
	dbMetaInfo := storeInfo.ManagerStoreMetaInfo()
	_, err := ets.LTNGDBEngine.CreateStore(ets.Ctx, storeInfo)
	require.NoError(b, err)

	store, err := ets.LTNGDBEngine.LoadStore(ets.Ctx, storeInfo)
	require.NoError(b, err)
	require.NotNil(b, store)

	{
		b.Log("CreateItem")

		bd := testbench.BenchData{}
		for _, user := range users {
			bd.Count()
			bvs := data.GetUserBytesValues(b, ets.TS(), user)

			item := &ltng_engine_v1.Item{
				Key:   bvs.BsKey,
				Value: bvs.BsValue,
			}
			opts := &ltng_engine_v1.IndexOpts{
				HasIdx:       true,
				ParentKey:    bvs.BsKey,
				IndexingKeys: [][]byte{bvs.BsKey, bvs.SecondaryIndexBs},
			}
			b.ResetTimer()
			b.StartTimer()
			_, err = ets.LTNGDBEngine.CreateItem(ets.Ctx, dbMetaInfo, item, opts)
			b.StopTimer()
			elapsed := b.Elapsed()
			bd.CalcAvg(elapsed)
			b.ResetTimer()
			assert.NoError(b, err)
		}

		b.Logf("%s", bd.String())
	}

	{
		pagination := &ltngenginemodels.Pagination{
			PageID:           1,
			PageSize:         10,
			PaginationCursor: 0,
		}
		opts := &ltng_engine_v1.IndexOpts{
			IndexProperties: ltng_engine_v1.IndexProperties{
				ListSearchPattern: ltng_engine_v1.Default,
			},
		}
		b.StartTimer()
		_, err = ets.LTNGDBEngine.ListItems(ets.Ctx, dbMetaInfo, pagination, opts)
		b.StopTimer()
		assert.NoError(b, err)
		b.Log(b.Elapsed())
		b.ResetTimer()
	}
}

func Benchmark_LTNGDB_Concurrent_Engine(b *testing.B) {
	users = data.GenerateRandomUsers(b, 50)
	ets = data.InitEngineTestSuite(b)
	b.Log("Benchmark_LTNGDB_Engine")
	benchmarkLTNGDBConcurrentEngine(b)
}

func benchmarkLTNGDBConcurrentEngine(b *testing.B) {
	storeInfo := &ltng_engine_concurrent_v1.StoreInfo{
		Name: "user-store",
		Path: "user-store",
	}
	dbMetaInfo := storeInfo.ManagerStoreMetaInfo()
	_, err := ets.LTNGDBConcurrentEngine.CreateStore(ets.Ctx, storeInfo)
	require.NoError(b, err)

	store, err := ets.LTNGDBConcurrentEngine.LoadStore(ets.Ctx, storeInfo)
	require.NoError(b, err)
	require.NotNil(b, store)

	{
		b.Log("CreateItem")

		bd := testbench.BenchData{}
		for _, user := range users {
			bd.Count()
			bvs := data.GetUserBytesValues(b, ets.TS(), user)

			item := &ltng_engine_concurrent_v1.Item{
				Key:   bvs.BsKey,
				Value: bvs.BsValue,
			}
			opts := &ltng_engine_concurrent_v1.IndexOpts{
				HasIdx:       true,
				ParentKey:    bvs.BsKey,
				IndexingKeys: [][]byte{bvs.BsKey, bvs.SecondaryIndexBs},
			}
			b.ResetTimer()
			b.StartTimer()
			_, err = ets.LTNGDBConcurrentEngine.CreateItem(ets.Ctx, dbMetaInfo, item, opts)
			b.StopTimer()
			elapsed := b.Elapsed()
			bd.CalcAvg(elapsed)
			b.ResetTimer()
			assert.NoError(b, err)
		}

		b.Logf("%s", bd.String())
	}

	{
		pagination := &ltngenginemodels.Pagination{
			PageID:           1,
			PageSize:         10,
			PaginationCursor: 0,
		}
		opts := &ltng_engine_concurrent_v1.IndexOpts{
			IndexProperties: ltng_engine_concurrent_v1.IndexProperties{
				ListSearchPattern: ltng_engine_concurrent_v1.Default,
			},
		}
		b.StartTimer()
		_, err = ets.LTNGDBConcurrentEngine.ListItems(ets.Ctx, dbMetaInfo, pagination, opts)
		b.StopTimer()
		assert.NoError(b, err)
		b.Log(b.Elapsed())
		b.ResetTimer()
	}
}

func Benchmark_LTNGDB_Engine_V2(b *testing.B) {
	users = data.GenerateRandomUsers(b, 50)
	ets = data.InitEngineTestSuite(b)
	b.Log("Benchmark_LTNGDB_Engine_V2")
	benchmarkLTNGDBEngineV2(b)
}

func benchmarkLTNGDBEngineV2(b *testing.B) {
	storeInfo := &ltngenginemodels.StoreInfo{
		Name: "user-store",
		Path: "user-store",
	}
	dbMetaInfo := storeInfo.ManagerStoreMetaInfo()
	_, err := ets.LTNGDBEngineV2.CreateStore(ets.Ctx, storeInfo)
	require.NoError(b, err)

	store, err := ets.LTNGDBEngineV2.LoadStore(ets.Ctx, storeInfo)
	require.NoError(b, err)
	require.NotNil(b, store)

	{
		b.Log("CreateItem")

		bd := testbench.BenchData{}
		for _, user := range users {
			bd.Count()
			bvs := data.GetUserBytesValues(b, ets.TS(), user)

			item := &ltngenginemodels.Item{
				Key:   bvs.BsKey,
				Value: bvs.BsValue,
			}
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:       true,
				ParentKey:    bvs.BsKey,
				IndexingKeys: [][]byte{bvs.BsKey, bvs.SecondaryIndexBs},
			}
			b.ResetTimer()
			b.StartTimer()
			_, err = ets.LTNGDBEngineV2.CreateItem(ets.Ctx, dbMetaInfo, item, opts)
			b.StopTimer()
			elapsed := b.Elapsed()
			bd.CalcAvg(elapsed)
			b.ResetTimer()
			assert.NoError(b, err)
		}

		b.Logf("%s", bd.String())
	}

	//ets.LTNGDBEngineV2.Close()

	{
		pagination := &ltngenginemodels.Pagination{
			PageID:           1,
			PageSize:         10,
			PaginationCursor: 0,
		}
		opts := &ltngenginemodels.IndexOpts{
			IndexProperties: ltngenginemodels.IndexProperties{
				ListSearchPattern: ltngenginemodels.Default,
			},
		}
		b.StartTimer()
		_, err = ets.LTNGDBEngineV2.ListItems(ets.Ctx, dbMetaInfo, pagination, opts)
		b.StopTimer()
		assert.NoError(b, err)
		b.Log(b.Elapsed())
		b.ResetTimer()
	}
}

func Benchmark_BadgerDB_Engine(b *testing.B) {
	users = data.GenerateRandomUsers(b, 50)
	ets = data.InitEngineTestSuite(b)
	b.Log("Benchmark_BadgerDB_Engine")
	benchmarkBadgerDBEngine(b)
}

func benchmarkBadgerDBEngine(b *testing.B) {
	dbInfo := &models_badgerdb_v4_management.DBInfo{
		Name:         "user-store",
		Path:         "user-store",
		CreatedAt:    time.Now(),
		LastOpenedAt: time.Now(),
	}

	dbMemoryInfo := dbInfo.InfoToMemoryInfo(ets.BadgerDBEngine.DB)

	err := ets.BadgerDBEngine.Manager.CreateStore(ets.Ctx, dbInfo)
	require.NoError(b, err)

	store, err := ets.BadgerDBEngine.Manager.GetDBInfo(ets.Ctx, dbInfo.Name)
	require.NoError(b, err)
	require.NotNil(b, store)

	{
		b.Log("CreateItem")

		bd := testbench.BenchData{}
		for _, user := range users {
			bd.Count()
			bvs := data.GetUserBytesValues(b, ets.TS(), user)
			item := &models_badgerdb_v4_operation.Item{
				Key:   bvs.BsKey,
				Value: bvs.BsValue,
			}
			opts := &models_badgerdb_v4_operation.IndexOpts{
				HasIdx:       true,
				ParentKey:    bvs.BsKey,
				IndexingKeys: [][]byte{bvs.BsKey, bvs.SecondaryIndexBs},
			}
			b.ResetTimer()
			b.StartTimer()
			err = ets.BadgerDBEngine.Operator.Operate(dbMemoryInfo).
				Create(ets.Ctx, item, opts, list_operator.DefaultRetrialOps)
			b.StopTimer()
			elapsed := b.Elapsed()
			bd.CalcAvg(elapsed)
			b.ResetTimer()
			assert.NoError(b, err)
		}

		b.Logf("%s", bd.String())
	}

	{
		opts := &models_badgerdb_v4_operation.IndexOpts{
			IndexProperties: models_badgerdb_v4_operation.IndexProperties{
				ListSearchPattern: models_badgerdb_v4_operation.Default,
			},
		}
		pagination := &models_badgerdb_v4_management.Pagination{
			PageID:   1,
			PageSize: 10,
		}
		b.StartTimer()
		_, err = ets.BadgerDBEngine.Operator.Operate(dbMemoryInfo).List(ets.Ctx, opts, pagination)
		b.StopTimer()
		assert.NoError(b, err)
		b.Log(b.Elapsed())
		b.ResetTimer()
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
	bs, err := execx.Executor(exec.Command(
		"sh", "-c",
		"find .ltngdb/v1/stores/user-store -maxdepth 1 -type f | wc -l",
	))
	require.NoError(t, err)
	t.Log(strings.TrimSpace(string(bs)))
}
