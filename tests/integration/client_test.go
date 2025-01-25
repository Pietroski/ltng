package integration_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.com/pietroski-software-company/devex/golang/concurrent"

	ltng_client "gitlab.com/pietroski-software-company/lightning-db/client"
	common_model "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/testbench"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
	"gitlab.com/pietroski-software-company/lightning-db/tests/data"
)

var (
	users []*data.User
	cts   *data.ClientTestSuite
)

func TestClientsLocally(t *testing.T) {
	data.CleanupDirectories(t)

	t.Log("TestLTNGDBClient")
	TestLTNGDBClient(t)

	t.Log("TestBadgerDBClient")
	TestBadgerDBClient(t)
}

func TestLTNGDBClient(t *testing.T) {
	var err error
	err = os.Setenv("LTNG_ENGINE", common_model.LightningEngineV2EngineVersionType.String())
	require.NoError(t, err)
	err = os.Setenv("LTNG_SERVER_PORT", "50050")
	require.NoError(t, err)
	err = os.Setenv("LTNG_SERVER_NETWORK", "tcp")
	require.NoError(t, err)
	err = os.Setenv("LTNG_UI_ADDR", "8080")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	offThread := concurrent.New("TestMain")
	offThread.Op(func() {
		main(ctx, cancel)
	})
	defer offThread.Wait()
	defer func() {
		time.Sleep(time.Millisecond * 500)
		cancel()
	}()

	time.Sleep(time.Millisecond * 500)

	users = data.GenerateRandomUsers(t, 50)
	cts = data.InitLocalClientTestSuite(t, common_model.LightningEngineV2EngineVersionType)

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

func TestBadgerDBClient(t *testing.T) {
	var err error
	err = os.Setenv("LTNG_ENGINE", common_model.BadgerDBV4EngineVersionType.String())
	require.NoError(t, err)
	err = os.Setenv("LTNG_SERVER_PORT", "50051")
	require.NoError(t, err)
	err = os.Setenv("LTNG_SERVER_NETWORK", "tcp")
	require.NoError(t, err)
	err = os.Setenv("LTNG_UI_ADDR", "8081")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	offThread := concurrent.New("TestMain")
	offThread.Op(func() {
		main(ctx, cancel)
	})
	defer offThread.Wait()
	defer func() {
		time.Sleep(time.Millisecond * 500)
		cancel()
	}()

	time.Sleep(time.Millisecond * 500)

	users = data.GenerateRandomUsers(t, 50)
	cts = data.InitLocalClientTestSuite(t, common_model.BadgerDBV4EngineVersionType)

	t.Log("Benchmark_BadgerDB_Client_Engine")
	testBadgerDBClient(t)
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
