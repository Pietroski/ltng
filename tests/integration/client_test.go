package integration_test

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

func TestClients(t *testing.T) {
	//_, err := ReadEnvFile(refToEnvFilename + envFilename)
	//require.NoError(t, err)
	//go func() {
	//	grpc.Main()
	//}()

	time.Sleep(2 * time.Second)

	users = data.GenerateRandomUsers(t, 50)
	cts = data.InitClientTestSuite(t)

	//t.Log("Benchmark_LTNGDB_Client_Engine")
	//testLTNGDBClient(t)

	t.Log("Benchmark_BadgerDB_Client_Engine")
	testBadgerDBClient(t)

	t.Log("Benchmark_LTNGDB_Client_Engine")
	testLTNGDBClient(t)
}

func testLTNGDBClient(t *testing.T) {
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
