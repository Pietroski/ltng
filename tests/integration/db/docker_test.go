package db_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.com/pietroski-software-company/golang/devex/testingx"

	ltng_client "gitlab.com/pietroski-software-company/lightning-db/client"
	search "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/common/search"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
	"gitlab.com/pietroski-software-company/lightning-db/tests/data"
)

func TestClientsWithinDocker(t *testing.T) {
	users = data.GenerateRandomUsers(t, 150)
	cts = data.InitClientTestSuite(t)

	t.Run("Test_LTNGDB_Client_Engine_Within_Docker", func(t *testing.T) {
		testLTNGDBClientWithinDocker(t)
	})

	t.Run("Test_BadgerDB_Client_Engine_Within_Docker", func(t *testing.T) {
		testBadgerDBClientWithinDocker(t)
	})
}

func testLTNGDBClientWithinDocker(t *testing.T) {
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

		bd := testingx.NewBenchSync()
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
			bd.CalcElapsedAvg(func() {
				_, err = cts.LTNGDBClient.Create(cts.Ctx, createRequest)
			})
			assert.NoError(t, err)
		}
		t.Log(bd)
	})

	t.Run("ListItems", func(t *testing.T) {
		t.Log("ListItems")

		bd := testingx.NewBenchSync()
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
			Pagination: &search.Pagination{
				PageId:   1,
				PageSize: 50,
			},
		}
		bd.CalcElapsedAvg(func() {
			_, err = cts.LTNGDBClient.List(cts.Ctx, listRequest)
		})
		assert.NoError(t, err)
		t.Log(bd)
	})

	t.Run("LoadItem", func(t *testing.T) {
		t.Log("LoadItem")

		bd := testingx.NewBenchSync()
		for _, user := range users {
			bvs := data.GetUserBytesValues(t, cts.TS(), user)
			loadRequest := &grpc_ltngdb.LoadRequest{
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: getStoreRequest.Name,
					DatabasePath: getStoreRequest.Path,
				},
				Item: &grpc_ltngdb.Item{
					Key: bvs.BsKey,
				},
			}
			bd.CalcElapsedAvg(func() {
				_, err = cts.LTNGDBClient.Load(cts.Ctx, loadRequest)
			})
			assert.NoError(t, err)
		}
		t.Log(bd)
	})

	t.Run("UpsertItem", func(t *testing.T) {
		t.Log("UpsertItem")

		bd := testingx.NewBenchSync()
		for _, user := range users {
			bvs := data.GetUserBytesValues(t, cts.TS(), user)
			upsertRequest := &grpc_ltngdb.UpsertRequest{
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
					IndexingKeys: [][]byte{bvs.BsKey, bvs.SecondaryIndexBs, bvs.ExtraUpsertIndex},
				},
			}
			bd.CalcElapsedAvg(func() {
				_, err = cts.LTNGDBClient.Upsert(cts.Ctx, upsertRequest)
			})
			assert.NoError(t, err)
		}
		t.Log(bd)
	})

	t.Run("DeleteItem", func(t *testing.T) {
		t.Log("DeleteItem")

		bd := testingx.NewBenchSync()
		for _, user := range users {
			bvs := data.GetUserBytesValues(t, cts.TS(), user)
			deleteRequest := &grpc_ltngdb.DeleteRequest{
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: getStoreRequest.Name,
					DatabasePath: getStoreRequest.Path,
				},
				Item: &grpc_ltngdb.Item{
					Key: bvs.BsKey,
				},
				IndexOpts: &grpc_ltngdb.IndexOpts{
					HasIdx:    true,
					ParentKey: bvs.BsKey,
					IndexingProperties: &grpc_ltngdb.IndexProperties{
						IndexDeletionBehaviour: grpc_ltngdb.IndexProperties_CASCADE,
					},
				},
			}
			bd.CalcElapsedAvg(func() {
				_, err = cts.LTNGDBClient.Delete(cts.Ctx, deleteRequest)
			})
			assert.NoError(t, err)
		}
		t.Log(bd)
	})
}

func testBadgerDBClientWithinDocker(t *testing.T) {
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

		bd := testingx.NewBenchSync()
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
			bd.CalcElapsedAvg(func() {
				_, err = cts.BadgerDBClient.Create(cts.Ctx, createRequest)
			})
			assert.NoError(t, err)
		}
		t.Log(bd)
	})

	t.Run("ListItems", func(t *testing.T) {
		t.Log("ListItems")

		bd := testingx.NewBenchSync()
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
			Pagination: &search.Pagination{
				PageId:   1,
				PageSize: 50,
			},
		}
		bd.CalcElapsedAvg(func() {
			_, err = cts.BadgerDBClient.List(cts.Ctx, listRequest)
		})
		assert.NoError(t, err)
		t.Log(bd)
	})

	t.Run("LoadItem", func(t *testing.T) {
		t.Log("LoadItem")

		bd := testingx.NewBenchSync()
		for _, user := range users {
			bvs := data.GetUserBytesValues(t, cts.TS(), user)
			loadRequest := &grpc_ltngdb.LoadRequest{
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: getStoreRequest.Name,
					DatabasePath: getStoreRequest.Path,
				},
				Item: &grpc_ltngdb.Item{
					Key: bvs.BsKey,
				},
			}
			bd.CalcElapsedAvg(func() {
				_, err = cts.BadgerDBClient.Load(cts.Ctx, loadRequest)
			})
			assert.NoError(t, err)
		}
		t.Log(bd)
	})

	t.Run("UpsertItem", func(t *testing.T) {
		t.Log("UpsertItem")

		bd := testingx.NewBenchSync()
		for _, user := range users {
			bvs := data.GetUserBytesValues(t, cts.TS(), user)
			upsertRequest := &grpc_ltngdb.UpsertRequest{
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
			}
			bd.CalcElapsedAvg(func() {
				_, err = cts.BadgerDBClient.Upsert(cts.Ctx, upsertRequest)
			})
			assert.NoError(t, err)
		}
		t.Log(bd)
	})

	t.Run("DeleteItem", func(t *testing.T) {
		t.Log("DeleteItem")

		bd := testingx.NewBenchSync()
		for _, user := range users {
			bvs := data.GetUserBytesValues(t, cts.TS(), user)
			deleteRequest := &grpc_ltngdb.DeleteRequest{
				DatabaseMetaInfo: &grpc_ltngdb.DatabaseMetaInfo{
					DatabaseName: getStoreRequest.Name,
					DatabasePath: getStoreRequest.Path,
				},
				Item: &grpc_ltngdb.Item{
					Key: bvs.BsKey,
				},
				IndexOpts: &grpc_ltngdb.IndexOpts{
					HasIdx:    true,
					ParentKey: bvs.BsKey,
					IndexingProperties: &grpc_ltngdb.IndexProperties{
						IndexDeletionBehaviour: grpc_ltngdb.IndexProperties_CASCADE,
					},
				},
			}
			bd.CalcElapsedAvg(func() {
				_, err = cts.BadgerDBClient.Delete(cts.Ctx, deleteRequest)
			})
			assert.NoError(t, err)
		}
		t.Log(bd)
	})
}
