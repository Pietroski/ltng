package benchmark

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ltng_client "gitlab.com/pietroski-software-company/lightning-db/client"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/testbench"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
	"gitlab.com/pietroski-software-company/lightning-db/tests/data"
)

var (
	users []*data.User
	cts   *data.ClientTestSuite
)

func TestClientsWithinDocker(t *testing.T) {
	users = data.GenerateRandomUsers(t, 150)
	cts = data.InitClientTestSuite(t)

	t.Run("Benchmark_LTNGDB_Client_Engine", func(t *testing.T) {
		testLTNGDBClient(t)
	})

	t.Run("Benchmark_BadgerDB_Client_Engine", func(t *testing.T) {
		testBadgerDBClient(t)
	})
}

func testLTNGDBClient(t *testing.T) {
	startTime := time.Now()
	defer func() {
		t.Logf("Total LTNGDB test duration: %v", time.Since(startTime))
	}()

	// Profiling is now handled at the TestClients level

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

	// Profiling is now handled at the TestClients level

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
