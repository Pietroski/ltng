package memorystorev1

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ltngdata "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	pagination "gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/testbench"
)

func TestLTNGCacheEngine(t *testing.T) {
	ts := setupTestSuite(t)
	storeInfo := &ltngdata.StoreInfo{
		Name: "user-store",
		Path: "user-store",
	}
	dbMetaInfo := storeInfo.ManagerStoreMetaInfo()

	{ // create
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				ParentKey:    bv.BsKey,
				IndexingKeys: [][]byte{bv.BsKey, bv.SecondaryIndexBs},
			}
			_, err := ts.cacheEngine.CreateItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
		}
	}

	{ // load
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
			require.Equal(t, bv.BsKey, fetchedItem.Key)
			require.Equal(t, bv.BsValue, fetchedItem.Value)
		}
	}

	{ // load - from parent key
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:    true,
				ParentKey: bv.BsKey,
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
			require.Equal(t, bv.BsKey, fetchedItem.Key)
		}
	}

	{ // load - from primary index
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.BsKey},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
			require.Equal(t, bv.BsKey, fetchedItem.Key)
		}
	}

	{ // load - from secondary index
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
			require.Equal(t, bv.BsKey, fetchedItem.Key)
		}
	}

	{ // load - and computational
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.BsKey, bv.SecondaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.AndComputational,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
			require.Equal(t, bv.BsKey, fetchedItem.Value)
		}
	}

	{ // load - or computational
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.OrComputational,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
			require.Equal(t, bv.BsKey, fetchedItem.Value)
		}
	}

	{ // list
		pagination := &pagination.Pagination{
			PageID:   1,
			PageSize: 200,
		}
		opts := &ltngdata.IndexOpts{
			IndexProperties: ltngdata.IndexProperties{
				ListSearchPattern: ltngdata.Default,
			},
		}
		fetchedItems, err := ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
		assert.NoError(t, err)
		assert.Len(t, fetchedItems.Items, 50)
		fetchedItemsMap := ltngdata.IndexListToMap(fetchedItems.Items)

		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			strKey := hex.EncodeToString(bv.Item.Key)
			_, ok := fetchedItemsMap[strKey]
			assert.True(t, ok)
		}
	}

	{ // list
		pagination := &pagination.Pagination{
			PageID:   1,
			PageSize: 50,
		}
		opts := &ltngdata.IndexOpts{
			IndexProperties: ltngdata.IndexProperties{
				ListSearchPattern: ltngdata.All,
			},
		}
		fetchedItems, err := ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
		assert.NoError(t, err)
		assert.Len(t, fetchedItems.Items, 50)
		fetchedItemsMap := ltngdata.IndexListToMap(fetchedItems.Items)

		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			strKey := hex.EncodeToString(bv.Item.Key)
			_, ok := fetchedItemsMap[strKey]
			assert.True(t, ok)
		}
	}

	{ // upsert
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				ParentKey:    bv.BsKey,
				IndexingKeys: [][]byte{bv.BsKey, bv.SecondaryIndexBs, bv.TertiaryIndexBs},
			}
			_, err := ts.cacheEngine.UpsertItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
		}
	}

	{ // load - from tertiary index
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.TertiaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
			require.Equal(t, bv.BsKey, fetchedItem.Key)
		}
	}

	{ // delete - index
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				IndexingKeys: [][]byte{bv.TertiaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexDeletionBehaviour: ltngdata.IndexOnly,
				},
			}
			_, err := ts.cacheEngine.DeleteItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
		}
	}

	{ // load & list indexes - from tertiary index
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.TertiaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(t, err)
			require.Nil(t, fetchedItem)

			pagination := &pagination.Pagination{
				PageID:   1,
				PageSize: 10,
			}
			opts = &ltngdata.IndexOpts{
				ParentKey: bv.BsKey,
				IndexProperties: ltngdata.IndexProperties{
					ListSearchPattern: ltngdata.IndexingList,
				},
			}
			fetchedItems, err := ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
			assert.NoError(t, err)
			assert.Len(t, fetchedItems.Items, 2)

			var primary, secondary, tertiary bool
			for _, item := range fetchedItems.Items {
				if bytes.Equal(bv.BsKey, item.Value) {
					primary = true
				} else if bytes.Equal(bv.SecondaryIndexBs, item.Value) {
					secondary = true
				} else if bytes.Equal(bv.TertiaryIndexBs, item.Value) {
					tertiary = true
				}
			}
			assert.True(t, primary)
			assert.True(t, secondary)
			assert.False(t, tertiary)
		}
	}

	{ // upsert
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				ParentKey:    bv.BsKey,
				IndexingKeys: [][]byte{bv.BsKey, bv.SecondaryIndexBs, bv.TertiaryIndexBs},
			}
			_, err := ts.cacheEngine.UpsertItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
		}
	}

	{ // load - from tertiary index
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.TertiaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
			require.Equal(t, bv.BsKey, fetchedItem.Key)
		}
	}

	{ // upsert
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				ParentKey:    bv.BsKey,
				IndexingKeys: [][]byte{bv.BsKey, bv.SecondaryIndexBs},
			}
			_, err := ts.cacheEngine.UpsertItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
		}
	}

	{ // load & list indexes - from tertiary index
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.TertiaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(t, err)
			require.Nil(t, fetchedItem)

			pagination := &pagination.Pagination{
				PageID:   1,
				PageSize: 10,
			}
			opts = &ltngdata.IndexOpts{
				ParentKey: bv.BsKey,
				IndexProperties: ltngdata.IndexProperties{
					ListSearchPattern: ltngdata.IndexingList,
				},
			}
			fetchedItems, err := ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
			assert.NoError(t, err)
			assert.Len(t, fetchedItems.Items, 2)

			var primary, secondary, tertiary bool
			for _, item := range fetchedItems.Items {
				if bytes.Equal(bv.BsKey, item.Value) {
					primary = true
				} else if bytes.Equal(bv.SecondaryIndexBs, item.Value) {
					secondary = true
				} else if bytes.Equal(bv.TertiaryIndexBs, item.Value) {
					tertiary = true
				}
			}
			assert.True(t, primary)
			assert.True(t, secondary)
			assert.False(t, tertiary)
		}
	}

	{ // delete - cascade by index
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexDeletionBehaviour: ltngdata.CascadeByIdx,
				},
			}
			_, err := ts.cacheEngine.DeleteItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
		}
	}

	{ // load
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(t, err)
			require.Nil(t, fetchedItem)
		}
	}

	{ // load - from parent key
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:    true,
				ParentKey: bv.BsKey,
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(t, err)
			require.Nil(t, fetchedItem)
		}
	}

	{ // load - from primary index
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.BsKey},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(t, err)
			require.Nil(t, fetchedItem)
		}
	}

	{ // load - from secondary index
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(t, err)
			require.Nil(t, fetchedItem)
		}
	}

	{ // load - and computational
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.BsKey, bv.SecondaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.AndComputational,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			assert.Error(t, err)
			require.Nil(t, fetchedItem)
		}
	}

	{ // load - or computational
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.OrComputational,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(t, err)
			require.Nil(t, fetchedItem)
		}
	}

	{ // list
		pagination := &pagination.Pagination{
			PageID:   1,
			PageSize: 200,
		}
		opts := &ltngdata.IndexOpts{
			IndexProperties: ltngdata.IndexProperties{
				ListSearchPattern: ltngdata.Default,
			},
		}
		fetchedItems, err := ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
		assert.NoError(t, err)
		assert.Len(t, fetchedItems.Items, 0)
	}

	{ // create
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				ParentKey:    bv.BsKey,
				IndexingKeys: [][]byte{bv.BsKey, bv.SecondaryIndexBs},
			}
			_, err := ts.cacheEngine.CreateItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
		}
	}

	{ // load
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
			require.Equal(t, bv.BsKey, fetchedItem.Key)
			require.Equal(t, bv.BsValue, fetchedItem.Value)
		}
	}

	{ // list
		pagination := &pagination.Pagination{
			PageID:   1,
			PageSize: 200,
		}
		opts := &ltngdata.IndexOpts{
			IndexProperties: ltngdata.IndexProperties{
				ListSearchPattern: ltngdata.Default,
			},
		}
		fetchedItems, err := ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
		assert.NoError(t, err)
		assert.Len(t, fetchedItems.Items, 50)
		fetchedItemsMap := ltngdata.IndexListToMap(fetchedItems.Items)

		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			strKey := hex.EncodeToString(bv.Item.Key)
			_, ok := fetchedItemsMap[strKey]
			assert.True(t, ok)
		}
	}

	{ // delete - cascade
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx: true,
				IndexProperties: ltngdata.IndexProperties{
					IndexDeletionBehaviour: ltngdata.Cascade,
				},
			}
			_, err := ts.cacheEngine.DeleteItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
		}
	}

	{ // load
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(t, err)
			require.Nil(t, fetchedItem)
		}
	}

	{ // load - from parent key
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:    true,
				ParentKey: bv.BsKey,
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(t, err)
			require.Nil(t, fetchedItem)
		}
	}

	{ // load - from primary index
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.BsKey},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(t, err)
			require.Nil(t, fetchedItem)
		}
	}

	{ // load - from secondary index
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(t, err)
			require.Nil(t, fetchedItem)
		}
	}

	{ // list
		pagination := &pagination.Pagination{
			PageID:   1,
			PageSize: 200,
		}
		opts := &ltngdata.IndexOpts{
			IndexProperties: ltngdata.IndexProperties{
				ListSearchPattern: ltngdata.Default,
			},
		}
		fetchedItems, err := ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
		assert.NoError(t, err)
		assert.Len(t, fetchedItems.Items, 0)
	}
}

func BenchmarkLTNGCacheEngine(b *testing.B) {
	ts := setupTestSuite(b)
	storeInfo := &ltngdata.StoreInfo{
		Name: "user-store",
		Path: "user-store",
	}
	dbMetaInfo := storeInfo.ManagerStoreMetaInfo()

	{ // create
		bd := testbench.New()
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			var err error
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				ParentKey:    bv.BsKey,
				IndexingKeys: [][]byte{bv.BsKey, bv.SecondaryIndexBs},
			}
			bd.CalcAvg(bd.CalcElapsed(func() {
				_, err = ts.cacheEngine.CreateItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			}))
			require.NoError(b, err)
		}
		b.Logf("create: %s\n", bd.String())
	}

	{ // load
		bd := testbench.New()
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			var fetchedItem *ltngdata.Item
			var err error
			opts := &ltngdata.IndexOpts{}
			bd.CalcAvg(bd.CalcElapsed(func() {
				fetchedItem, err = ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			}))
			require.NoError(b, err)
			require.Equal(b, bv.BsKey, fetchedItem.Key)
		}
		b.Logf("load: %s\n", bd.String())
	}

	{ // load - from parent key
		bd := testbench.New()
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			var fetchedItem *ltngdata.Item
			var err error
			opts := &ltngdata.IndexOpts{
				HasIdx:    true,
				ParentKey: bv.BsKey,
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			bd.CalcAvg(bd.CalcElapsed(func() {
				fetchedItem, err = ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			}))
			require.NoError(b, err)
			require.Equal(b, bv.BsKey, fetchedItem.Key)
		}
		b.Logf("load - from parent key: %s\n", bd.String())
	}

	{ // load - from primary index
		bd := testbench.New()
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			var fetchedItem *ltngdata.Item
			var err error
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.BsKey},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			bd.CalcAvg(bd.CalcElapsed(func() {
				fetchedItem, err = ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			}))
			require.NoError(b, err)
			require.Equal(b, bv.BsKey, fetchedItem.Key)
		}
		b.Logf("load - from primary index: %s\n", bd.String())
	}

	{ // load - from secondary index
		bd := testbench.New()
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			var fetchedItem *ltngdata.Item
			var err error
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			bd.CalcAvg(bd.CalcElapsed(func() {
				fetchedItem, err = ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			}))
			require.NoError(b, err)
			require.Equal(b, bv.BsKey, fetchedItem.Key)
		}
		b.Logf("load - from secondary index: %s\n", bd.String())
	}

	{ // load - and computational
		bd := testbench.New()
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.BsKey, bv.SecondaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.AndComputational,
				},
			}
			var fetchedItem *ltngdata.Item
			var err error
			bd.CalcAvg(bd.CalcElapsed(func() {
				fetchedItem, err = ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			}))
			require.NoError(b, err)
			require.Equal(b, bv.BsKey, fetchedItem.Value)
		}
		b.Logf("load - and computational: %s\n", bd.String())
	}

	{ // load - or computational
		bd := testbench.New()
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.OrComputational,
				},
			}
			var fetchedItem *ltngdata.Item
			var err error
			bd.CalcAvg(bd.CalcElapsed(func() {
				fetchedItem, err = ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			}))
			require.NoError(b, err)
			require.Equal(b, bv.BsKey, fetchedItem.Value)
		}
		b.Logf("load - or computational: %s\n", bd.String())
	}

	{ // list
		bd := testbench.New()
		pagination := &pagination.Pagination{
			PageID:   1,
			PageSize: 50,
		}
		opts := &ltngdata.IndexOpts{
			IndexProperties: ltngdata.IndexProperties{
				ListSearchPattern: ltngdata.Default,
			},
		}
		var err error
		var fetchedItems *ltngdata.ListItemsResult
		bd.CalcAvg(bd.CalcElapsed(func() {
			fetchedItems, err = ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
		}))
		assert.NoError(b, err)
		assert.Len(b, fetchedItems.Items, 50)
		fetchedItemsMap := ltngdata.IndexListToMap(fetchedItems.Items)

		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			strKey := hex.EncodeToString(bv.Item.Key)
			_, ok := fetchedItemsMap[strKey]
			assert.True(b, ok)
		}

		b.Logf("list default: %s\n", bd.String())
	}

	{ // list
		bd := testbench.New()
		pagination := &pagination.Pagination{
			PageID:   1,
			PageSize: 50,
		}
		opts := &ltngdata.IndexOpts{
			IndexProperties: ltngdata.IndexProperties{
				ListSearchPattern: ltngdata.All,
			},
		}
		var err error
		var fetchedItems *ltngdata.ListItemsResult
		bd.CalcAvg(bd.CalcElapsed(func() {
			fetchedItems, err = ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
		}))

		assert.NoError(b, err)
		assert.Len(b, fetchedItems.Items, 50)
		fetchedItemsMap := ltngdata.IndexListToMap(fetchedItems.Items)

		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			strKey := hex.EncodeToString(bv.Item.Key)
			_, ok := fetchedItemsMap[strKey]
			assert.True(b, ok)
		}

		b.Logf("list default: %s\n", bd.String())
	}

	{ // upsert
		bd := testbench.New()
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				ParentKey:    bv.BsKey,
				IndexingKeys: [][]byte{bv.BsKey, bv.SecondaryIndexBs, bv.TertiaryIndexBs},
			}
			var err error
			bd.CalcAvg(bd.CalcElapsed(func() {
				_, err = ts.cacheEngine.UpsertItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			}))
			require.NoError(b, err)
		}
		b.Logf("upsert: %s\n", bd.String())
	}

	{ // load - from tertiary index
		bd := testbench.New()
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.TertiaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			var err error
			var fetchedItem *ltngdata.Item
			bd.CalcAvg(bd.CalcElapsed(func() {
				fetchedItem, err = ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			}))
			require.NoError(b, err)
			require.Equal(b, bv.BsKey, fetchedItem.Key)
		}
		b.Logf("load: %s\n", bd.String())
	}

	{ // delete - index
		bd := testbench.New()
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				IndexingKeys: [][]byte{bv.TertiaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexDeletionBehaviour: ltngdata.IndexOnly,
				},
			}
			var err error
			bd.CalcAvg(bd.CalcElapsed(func() {
				_, err = ts.cacheEngine.DeleteItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			}))
			require.NoError(b, err)
		}
		b.Logf("delete - index only: %s\n", bd.String())
	}

	{ // load & list indexes - from tertiary index
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.TertiaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(b, err)
			require.Nil(b, fetchedItem)

			pagination := &pagination.Pagination{
				PageID:   1,
				PageSize: 10,
			}
			opts = &ltngdata.IndexOpts{
				ParentKey: bv.BsKey,
				IndexProperties: ltngdata.IndexProperties{
					ListSearchPattern: ltngdata.IndexingList,
				},
			}

			fetchedItems, err := ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
			assert.NoError(b, err)
			assert.Len(b, fetchedItems.Items, 2)

			var primary, secondary, tertiary bool
			for _, item := range fetchedItems.Items {
				if bytes.Equal(bv.BsKey, item.Value) {
					primary = true
				} else if bytes.Equal(bv.SecondaryIndexBs, item.Value) {
					secondary = true
				} else if bytes.Equal(bv.TertiaryIndexBs, item.Value) {
					tertiary = true
				}
			}
			assert.True(b, primary)
			assert.True(b, secondary)
			assert.False(b, tertiary)
		}
	}

	{ // upsert
		bd := testbench.New()
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				ParentKey:    bv.BsKey,
				IndexingKeys: [][]byte{bv.BsKey, bv.SecondaryIndexBs, bv.TertiaryIndexBs},
			}
			var err error
			bd.CalcAvg(bd.CalcElapsed(func() {
				_, err = ts.cacheEngine.UpsertItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			}))
			require.NoError(b, err)
		}
		b.Logf("upsert: %s\n", bd.String())
	}

	{ // load - from tertiary index
		bd := testbench.New()
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.TertiaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			var err error
			var fetchedItem *ltngdata.Item
			bd.CalcAvg(bd.CalcElapsed(func() {
				fetchedItem, err = ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			}))
			require.NoError(b, err)
			require.Equal(b, bv.BsKey, fetchedItem.Key)
		}
		b.Logf("load: %s\n", bd.String())
	}

	{ // upsert
		bd := testbench.New()
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				ParentKey:    bv.BsKey,
				IndexingKeys: [][]byte{bv.BsKey, bv.SecondaryIndexBs},
			}
			var err error
			bd.CalcAvg(bd.CalcElapsed(func() {
				_, err = ts.cacheEngine.UpsertItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			}))
			require.NoError(b, err)
		}
		b.Logf("upsert: %s\n", bd.String())
	}

	{ // load & list indexes - from tertiary index
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.TertiaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(b, err)
			require.Nil(b, fetchedItem)

			pagination := &pagination.Pagination{
				PageID:   1,
				PageSize: 10,
			}
			opts = &ltngdata.IndexOpts{
				ParentKey: bv.BsKey,
				IndexProperties: ltngdata.IndexProperties{
					ListSearchPattern: ltngdata.IndexingList,
				},
			}
			fetchedItems, err := ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
			assert.NoError(b, err)
			assert.Len(b, fetchedItems.Items, 2)

			var primary, secondary, tertiary bool
			for _, item := range fetchedItems.Items {
				if bytes.Equal(bv.BsKey, item.Value) {
					primary = true
				} else if bytes.Equal(bv.SecondaryIndexBs, item.Value) {
					secondary = true
				} else if bytes.Equal(bv.TertiaryIndexBs, item.Value) {
					tertiary = true
				}
			}
			assert.True(b, primary)
			assert.True(b, secondary)
			assert.False(b, tertiary)
		}
	}

	{ // delete - cascade by index
		bd := testbench.New()
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexDeletionBehaviour: ltngdata.CascadeByIdx,
				},
			}
			var err error
			bd.CalcAvg(bd.CalcElapsed(func() {
				_, err = ts.cacheEngine.DeleteItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			}))
			require.NoError(b, err)
		}
		b.Logf("delete - cascade by index: %s\n", bd.String())
	}

	{ // load
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(b, err)
			require.Nil(b, fetchedItem)
		}
	}

	{ // load - from parent key
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:    true,
				ParentKey: bv.BsKey,
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(b, err)
			require.Nil(b, fetchedItem)
		}
	}

	{ // load - from primary index
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.BsKey},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(b, err)
			require.Nil(b, fetchedItem)
		}
	}

	{ // load - from secondary index
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(b, err)
			require.Nil(b, fetchedItem)
		}
	}

	{ // load - and computational
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.BsKey, bv.SecondaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.AndComputational,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			assert.Error(b, err)
			require.Nil(b, fetchedItem)
		}
	}

	{ // load - or computational
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.OrComputational,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(b, err)
			require.Nil(b, fetchedItem)
		}
	}

	{ // list
		pagination := &pagination.Pagination{
			PageID:   1,
			PageSize: 200,
		}
		opts := &ltngdata.IndexOpts{
			IndexProperties: ltngdata.IndexProperties{
				ListSearchPattern: ltngdata.Default,
			},
		}
		fetchedItems, err := ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
		assert.NoError(b, err)
		assert.Len(b, fetchedItems.Items, 0)
	}

	{ // create
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				ParentKey:    bv.BsKey,
				IndexingKeys: [][]byte{bv.BsKey, bv.SecondaryIndexBs},
			}
			_, err := ts.cacheEngine.CreateItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(b, err)
		}
	}

	{ // load
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(b, err)
			require.Equal(b, bv.BsKey, fetchedItem.Key)
			require.Equal(b, bv.BsValue, fetchedItem.Value)
		}
	}

	{ // list
		pagination := &pagination.Pagination{
			PageID:   1,
			PageSize: 200,
		}
		opts := &ltngdata.IndexOpts{
			IndexProperties: ltngdata.IndexProperties{
				ListSearchPattern: ltngdata.Default,
			},
		}
		fetchedItems, err := ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
		assert.NoError(b, err)
		assert.Len(b, fetchedItems.Items, 50)
		fetchedItemsMap := ltngdata.IndexListToMap(fetchedItems.Items)

		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			strKey := hex.EncodeToString(bv.Item.Key)
			_, ok := fetchedItemsMap[strKey]
			assert.True(b, ok)
		}
	}

	{ // delete - cascade
		bd := testbench.New()
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx: true,
				IndexProperties: ltngdata.IndexProperties{
					IndexDeletionBehaviour: ltngdata.Cascade,
				},
			}
			var err error
			bd.CalcAvg(bd.CalcElapsed(func() {
				_, err = ts.cacheEngine.DeleteItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			}))
			require.NoError(b, err)
		}
		b.Logf("delete - cascade: %s\n", bd.String())
	}

	{ // load
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(b, err)
			require.Nil(b, fetchedItem)
		}
	}

	{ // load - from parent key
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:    true,
				ParentKey: bv.BsKey,
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(b, err)
			require.Nil(b, fetchedItem)
		}
	}

	{ // load - from primary index
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.BsKey},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(b, err)
			require.Nil(b, fetchedItem)
		}
	}

	{ // load - from secondary index
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			opts := &ltngdata.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngdata.IndexProperties{
					IndexSearchPattern: ltngdata.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(b, err)
			require.Nil(b, fetchedItem)
		}
	}

	{ // list
		pagination := &pagination.Pagination{
			PageID:   1,
			PageSize: 200,
		}
		opts := &ltngdata.IndexOpts{
			IndexProperties: ltngdata.IndexProperties{
				ListSearchPattern: ltngdata.Default,
			},
		}
		fetchedItems, err := ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
		assert.NoError(b, err)
		assert.Len(b, fetchedItems.Items, 0)
	}
}
