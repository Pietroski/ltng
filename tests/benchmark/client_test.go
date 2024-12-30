package benchmark

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ltng_client "gitlab.com/pietroski-software-company/lightning-db/client"
	grpc_pagination "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/common/search"
	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
	"testing"

	"gitlab.com/pietroski-software-company/lightning-db/tests/data"
)

var (
	users []*data.User
	cts   *data.ClientTestSuite
)

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
			createRequest := &grpc_ltngdb.UpsertRequest{
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
			_, err = cts.LTNGDBClient.Upsert(cts.Ctx, createRequest)
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
			createRequest := &grpc_ltngdb.UpsertRequest{
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
			_, err = cts.BadgerDBClient.Upsert(cts.Ctx, createRequest)
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
