package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ltng_client "gitlab.com/pietroski-software-company/lightning-db/client"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/testbench"
	grpc_pagination "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/common/search"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
	"gitlab.com/pietroski-software-company/lightning-db/tests/data"
)

var (
	users []*data.User
	cts   *data.ClientTestSuite
	profileDir = filepath.Join("docs", "profiles")
)

func setupProfiles(t testing.TB, testName string) (*os.File, *os.File) {
	err := os.MkdirAll(profileDir, 0755)
	require.NoError(t, err)

	cpuFile, err := os.Create(filepath.Join(profileDir, fmt.Sprintf("%s-cpu.prof", testName)))
	require.NoError(t, err)
	err = pprof.StartCPUProfile(cpuFile)
	require.NoError(t, err)

	runtime.GC()
	memFile, err := os.Create(filepath.Join(profileDir, fmt.Sprintf("%s-mem.prof", testName)))
	require.NoError(t, err)
	err = pprof.WriteHeapProfile(memFile)
	require.NoError(t, err)

	return cpuFile, memFile
}

func measureNetworkLatency(t testing.TB, operation func() error) (time.Duration, error) {
	start := time.Now()
	err := operation()
	latency := time.Since(start)
	return latency, err
}

func BenchmarkAllClients(b *testing.B) {
	users = data.GenerateRandomUsers(b, 50)
	cts = data.InitClientTestSuite(b)

	b.Log("Benchmark_LTNGDB_Client_Engine")
	Benchmark_LTNGDB_Client_Engine(b)

	b.Log("Benchmark_BadgerDB_Client_Engine")
	Benchmark_BadgerDB_Client_Engine(b)
}

func Benchmark_LTNGDB_Client_Engine(b *testing.B) {
	createStoreRequest := &grpc_ltngdb.CreateStoreRequest{
		Name: "user-store",
		Path: "user-store",
	}
	_, err := cts.LTNGDBClient.CreateStore(cts.Ctx, createStoreRequest)
	require.NoError(b, err)

	getStoreRequest := &grpc_ltngdb.GetStoreRequest{
		Name: "user-store",
		Path: "user-store",
	}
	store, err := cts.LTNGDBClient.GetStore(cts.Ctx, getStoreRequest)
	require.NoError(b, err)
	require.NotNil(b, store)

	b.Run("CreateItem", func(b *testing.B) {
		b.Log("CreateItem")

		for _, user := range users {
			bvs := data.GetUserBytesValues(b, cts.TS(), user)
			createRequest := &grpc_ltngdb.CreateRequest{
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: getStoreRequest.Name,
					DatabasePath: getStoreRequest.Path,
				},
				Item: &grpc_ltngdb.Item{
					Key:   bvs.BsKey,
					Value: bvs.BsValue,
				},
				IndexOpts: &grpc_ltngdb.IndexOpts{
					HasIdx:       true,
					ParentKey:    bvs.BsKey,
					IndexingKeys: [][]byte{bvs.BsKey, bvs.SecondaryIndexBs},
				},
				RetrialOpts: ltng_client.DefaultRetrialOpts,
			}
			b.StartTimer()
			_, err = cts.LTNGDBClient.Create(cts.Ctx, createRequest)
			b.StopTimer()
			assert.NoError(b, err)
			b.Log(b.Elapsed())
			b.ResetTimer()
		}
	})

	{
		listRequest := &grpc_ltngdb.ListRequest{
			DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
				DatabaseName: getStoreRequest.Name,
				DatabasePath: getStoreRequest.Path,
			},
			IndexOpts: &grpc_ltngdb.IndexOpts{
				IndexingProperties: &grpc_ltngdb.IndexProperties{
					ListSearchPattern: grpc_ltngdb.IndexProperties_DEFAULT,
				},
			},
			Pagination: &grpc_pagination.Pagination{
				PageId:           1,
				PageSize:         10,
				PaginationCursor: 0,
			},
		}
		b.StartTimer()
		_, err = cts.LTNGDBClient.List(cts.Ctx, listRequest)
		b.StopTimer()
		assert.NoError(b, err)
		b.Log(b.Elapsed())
		b.ResetTimer()
	}

	b.Run("LoadItem", func(b *testing.B) {
		b.Log("LoadItem")
	})

	b.Run("DeleteItem", func(b *testing.B) {
		b.Log("DeleteItem")
	})
}

func Benchmark_BadgerDB_Client_Engine(b *testing.B) {
	createStoreRequest := &grpc_ltngdb.CreateStoreRequest{
		Name: "user-store",
		Path: "user-store",
	}
	_, err := cts.BadgerDBClient.CreateStore(cts.Ctx, createStoreRequest)
	require.NoError(b, err)

	getStoreRequest := &grpc_ltngdb.GetStoreRequest{
		Name: "user-store",
		Path: "user-store",
	}
	store, err := cts.BadgerDBClient.GetStore(cts.Ctx, getStoreRequest)
	require.NoError(b, err)
	require.NotNil(b, store)

	b.Run("CreateItem", func(b *testing.B) {
		b.Log("CreateItem")

		for _, user := range users {
			bvs := data.GetUserBytesValues(b, cts.TS(), user)
			createRequest := &grpc_ltngdb.CreateRequest{
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: getStoreRequest.Name,
					DatabasePath: getStoreRequest.Path,
				},
				Item: &grpc_ltngdb.Item{
					Key:   bvs.BsKey,
					Value: bvs.BsValue,
				},
				IndexOpts: &grpc_ltngdb.IndexOpts{
					HasIdx:       true,
					ParentKey:    bvs.BsKey,
					IndexingKeys: [][]byte{bvs.BsKey, bvs.SecondaryIndexBs},
				},
				RetrialOpts: ltng_client.DefaultRetrialOpts,
			}
			b.StartTimer()
			_, err = cts.BadgerDBClient.Create(cts.Ctx, createRequest)
			b.StopTimer()
			assert.NoError(b, err)
			b.Log(b.Elapsed())
			b.ResetTimer()
		}
	})

	{
		listRequest := &grpc_ltngdb.ListRequest{
			DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
				DatabaseName: getStoreRequest.Name,
				DatabasePath: getStoreRequest.Path,
			},
			IndexOpts: &grpc_ltngdb.IndexOpts{
				IndexingProperties: &grpc_ltngdb.IndexProperties{
					ListSearchPattern: grpc_ltngdb.IndexProperties_DEFAULT,
				},
			},
			Pagination: &grpc_pagination.Pagination{
				PageId:           1,
				PageSize:         10,
				PaginationCursor: 0,
			},
		}
		b.StartTimer()
		_, err = cts.BadgerDBClient.List(cts.Ctx, listRequest)
		b.StopTimer()
		assert.NoError(b, err)
		b.Log(b.Elapsed())
		b.ResetTimer()
	}

	b.Run("LoadItem", func(b *testing.B) {
		b.Log("LoadItem")
	})

	b.Run("DeleteItem", func(b *testing.B) {
		b.Log("DeleteItem")
	})
}

func TestClients(t *testing.T) {
	users = data.GenerateRandomUsers(t, 50)
	cts = data.InitClientTestSuite(t)

	// Setup profiling for BadgerDB
	cpuBadger, memBadger := setupProfiles(t, "badgerdb")
	defer cpuBadger.Close()
	defer pprof.StopCPUProfile()

	t.Log("Benchmark_BadgerDB_Client_Engine")
	testBadgerDBClient(t)

	// Write final memory profile for BadgerDB
	pprof.WriteHeapProfile(memBadger)
	memBadger.Close()

	// Setup profiling for LTNGDB
	cpuLTNG, memLTNG := setupProfiles(t, "ltngdb")
	defer cpuLTNG.Close()
	defer pprof.StopCPUProfile()

	t.Log("Benchmark_LTNGDB_Client_Engine")
	testLTNGDBClient(t)

	// Write final memory profile for LTNGDB
	pprof.WriteHeapProfile(memLTNG)
	memLTNG.Close()

	// Run network latency tests
	t.Run("NetworkLatency", func(t *testing.T) {
		TestNetworkLatency(t)
	})
}

func testLTNGDBClient(t *testing.T) {
	startTime := time.Now()
	defer func() {
		t.Logf("Total LTNGDB test duration: %v", time.Since(startTime))
	}()
	createStoreRequest := &grpc_ltngdb.CreateStoreRequest{
		Name: "user-store",
		Path: "user-store",
	}
	_, err := cts.LTNGDBClient.CreateStore(cts.Ctx, createStoreRequest)
	require.NoError(t, err)

	getStoreRequest := &grpc_ltngdb.GetStoreRequest{
		Name: "user-store",
		Path: "user-store",
	}
	store, err := cts.LTNGDBClient.GetStore(cts.Ctx, getStoreRequest)
	require.NoError(t, err)
	require.NotNil(t, store)

	t.Run("CreateItem", func(t *testing.T) {
		t.Log("CreateItem")

		tb := testbench.New()
		for _, user := range users {
			bvs := data.GetUserBytesValues(t, cts.TS(), user)
			createRequest := &grpc_ltngdb.CreateRequest{
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: getStoreRequest.Name,
					DatabasePath: getStoreRequest.Path,
				},
				Item: &grpc_ltngdb.Item{
					Key:   bvs.BsKey,
					Value: bvs.BsValue,
				},
				IndexOpts: &grpc_ltngdb.IndexOpts{
					HasIdx:       true,
					ParentKey:    bvs.BsKey,
					IndexingKeys: [][]byte{bvs.BsKey, bvs.SecondaryIndexBs},
				},
				RetrialOpts: ltng_client.DefaultRetrialOpts,
			}
			tb.CalcAvg(tb.CalcElapsed(func() {
				_, err = cts.LTNGDBClient.Create(cts.Ctx, createRequest)
			}))
			assert.NoError(t, err)
		}
		t.Log(tb)
	})
}

func testBadgerDBClient(t *testing.T) {
	startTime := time.Now()
	defer func() {
		t.Logf("Total BadgerDB test duration: %v", time.Since(startTime))
	}()
	createStoreRequest := &grpc_ltngdb.CreateStoreRequest{
		Name: "user-store",
		Path: "user-store",
	}
	_, err := cts.BadgerDBClient.CreateStore(cts.Ctx, createStoreRequest)
	require.NoError(t, err)

	getStoreRequest := &grpc_ltngdb.GetStoreRequest{
		Name: "user-store",
		Path: "user-store",
	}
	store, err := cts.BadgerDBClient.GetStore(cts.Ctx, getStoreRequest)
	require.NoError(t, err)
	require.NotNil(t, store)

	t.Run("CreateItem", func(t *testing.T) {
		t.Log("CreateItem")

		tb := testbench.New()
		for _, user := range users {
			bvs := data.GetUserBytesValues(t, cts.TS(), user)
			createRequest := &grpc_ltngdb.CreateRequest{
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: getStoreRequest.Name,
					DatabasePath: getStoreRequest.Path,
				},
				Item: &grpc_ltngdb.Item{
					Key:   bvs.BsKey,
					Value: bvs.BsValue,
				},
				IndexOpts: &grpc_ltngdb.IndexOpts{
					HasIdx:       true,
					ParentKey:    bvs.BsKey,
					IndexingKeys: [][]byte{bvs.BsKey, bvs.SecondaryIndexBs},
				},
				RetrialOpts: ltng_client.DefaultRetrialOpts,
			}
			tb.CalcAvg(tb.CalcElapsed(func() {
				_, err = cts.BadgerDBClient.Create(cts.Ctx, createRequest)
			}))
			assert.NoError(t, err)
		}
		t.Log(tb)
	})
}
