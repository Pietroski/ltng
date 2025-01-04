package memorystorev1

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/testbench"
)

func TestLTNGCacheEngine(t *testing.T) {
	ts := setupTestSuite(t)
	storeInfo := &ltngenginemodels.StoreInfo{
		Name: "user-store",
		Path: "user-store",
	}
	dbMetaInfo := storeInfo.ManagerStoreMetaInfo()

	{ // create
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngenginemodels.IndexOpts{
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
			opts := &ltngenginemodels.IndexOpts{}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
			require.Equal(t, bv.BsKey, fetchedItem.Key)
			require.Equal(t, bv.BsValue, fetchedItem.Value)
		}
	}

	{ // load - from parent key
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:    true,
				ParentKey: bv.BsKey,
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexSearchPattern: ltngenginemodels.One,
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
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.BsKey},
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexSearchPattern: ltngenginemodels.One,
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
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexSearchPattern: ltngenginemodels.One,
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
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.BsKey, bv.SecondaryIndexBs},
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexSearchPattern: ltngenginemodels.AndComputational,
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
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexSearchPattern: ltngenginemodels.OrComputational,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
			require.Equal(t, bv.BsKey, fetchedItem.Value)
		}
	}

	{ // list
		pagination := &ltngenginemodels.Pagination{
			PageID:   1,
			PageSize: 50,
		}
		opts := &ltngenginemodels.IndexOpts{
			IndexProperties: ltngenginemodels.IndexProperties{
				ListSearchPattern: ltngenginemodels.Default,
			},
		}
		fetchedItems, err := ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
		assert.NoError(t, err)
		assert.Len(t, fetchedItems.Items, 50)
		fetchedItemsMap := ltngenginemodels.IndexListToMap(fetchedItems.Items)

		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			strKey := hex.EncodeToString(bv.Item.Key)
			_, ok := fetchedItemsMap[strKey]
			assert.True(t, ok)
		}
	}

	{ // list
		pagination := &ltngenginemodels.Pagination{
			PageID:   1,
			PageSize: 50,
		}
		opts := &ltngenginemodels.IndexOpts{
			IndexProperties: ltngenginemodels.IndexProperties{
				ListSearchPattern: ltngenginemodels.All,
			},
		}
		fetchedItems, err := ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
		assert.NoError(t, err)
		assert.Len(t, fetchedItems.Items, 50)
		fetchedItemsMap := ltngenginemodels.IndexListToMap(fetchedItems.Items)

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
			opts := &ltngenginemodels.IndexOpts{
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
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.TertiaryIndexBs},
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexSearchPattern: ltngenginemodels.One,
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
			opts := &ltngenginemodels.IndexOpts{
				IndexingKeys: [][]byte{bv.TertiaryIndexBs},
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexDeletionBehaviour: ltngenginemodels.IndexOnly,
				},
			}
			_, err := ts.cacheEngine.DeleteItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
		}
	}

	{ // load & list indexes - from tertiary index
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.TertiaryIndexBs},
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexSearchPattern: ltngenginemodels.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(t, err)
			require.Nil(t, fetchedItem)

			pagination := &ltngenginemodels.Pagination{
				PageID:   1,
				PageSize: 10,
			}
			opts = &ltngenginemodels.IndexOpts{
				ParentKey: bv.BsKey,
				IndexProperties: ltngenginemodels.IndexProperties{
					ListSearchPattern: ltngenginemodels.IndexingList,
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
			opts := &ltngenginemodels.IndexOpts{
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
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.TertiaryIndexBs},
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexSearchPattern: ltngenginemodels.One,
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
			opts := &ltngenginemodels.IndexOpts{
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
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.TertiaryIndexBs},
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexSearchPattern: ltngenginemodels.One,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(t, err)
			require.Nil(t, fetchedItem)

			pagination := &ltngenginemodels.Pagination{
				PageID:   1,
				PageSize: 10,
			}
			opts = &ltngenginemodels.IndexOpts{
				ParentKey: bv.BsKey,
				IndexProperties: ltngenginemodels.IndexProperties{
					ListSearchPattern: ltngenginemodels.IndexingList,
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
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexDeletionBehaviour: ltngenginemodels.CascadeByIdx,
				},
			}
			_, err := ts.cacheEngine.DeleteItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.NoError(t, err)
		}
	}

	{ // load
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngenginemodels.IndexOpts{}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(t, err)
			require.Nil(t, fetchedItem)
		}
	}

	{ // load - from parent key
		for _, user := range ts.users {
			bv := GetUserBytesValues(t, ts.testsuite, user)
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:    true,
				ParentKey: bv.BsKey,
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexSearchPattern: ltngenginemodels.One,
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
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.BsKey},
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexSearchPattern: ltngenginemodels.One,
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
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexSearchPattern: ltngenginemodels.One,
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
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.BsKey, bv.SecondaryIndexBs},
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexSearchPattern: ltngenginemodels.AndComputational,
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
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexSearchPattern: ltngenginemodels.OrComputational,
				},
			}
			fetchedItem, err := ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
			require.Error(t, err)
			require.Nil(t, fetchedItem)
		}
	}

	{ // list
		pagination := &ltngenginemodels.Pagination{
			PageID:   1,
			PageSize: 200,
		}
		opts := &ltngenginemodels.IndexOpts{
			IndexProperties: ltngenginemodels.IndexProperties{
				ListSearchPattern: ltngenginemodels.Default,
			},
		}
		fetchedItems, err := ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
		assert.NoError(t, err)
		assert.Len(t, fetchedItems.Items, 0)
	}

	// delete idx - ok
	// load exp error - ok
	// delete cascade by idx
	// load
	// list
	// re-create
	// delete cascade
	// load
	// list
}

func BenchmarkLTNGCacheEngine(b *testing.B) {
	ts := setupTestSuite(b)
	storeInfo := &ltngenginemodels.StoreInfo{
		Name: "user-store",
		Path: "user-store",
	}
	dbMetaInfo := storeInfo.ManagerStoreMetaInfo()

	{ // create
		bd := testbench.New()
		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			var err error
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:       true,
				ParentKey:    bv.BsKey,
				IndexingKeys: [][]byte{bv.BsKey, bv.SecondaryIndexBs},
			}
			bd.CalcAvg(
				bd.CalcElapsed(func() {
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
			var fetchedItem *ltngenginemodels.Item
			var err error
			opts := &ltngenginemodels.IndexOpts{}
			bd.CalcAvg(
				bd.CalcElapsed(func() {
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
			var fetchedItem *ltngenginemodels.Item
			var err error
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:    true,
				ParentKey: bv.BsKey,
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexSearchPattern: ltngenginemodels.One,
				},
			}
			bd.CalcAvg(
				bd.CalcElapsed(func() {
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
			var fetchedItem *ltngenginemodels.Item
			var err error
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.BsKey},
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexSearchPattern: ltngenginemodels.One,
				},
			}
			bd.CalcAvg(
				bd.CalcElapsed(func() {
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
			var fetchedItem *ltngenginemodels.Item
			var err error
			opts := &ltngenginemodels.IndexOpts{
				HasIdx:       true,
				IndexingKeys: [][]byte{bv.SecondaryIndexBs},
				IndexProperties: ltngenginemodels.IndexProperties{
					IndexSearchPattern: ltngenginemodels.One,
				},
			}
			bd.CalcAvg(
				bd.CalcElapsed(func() {
					fetchedItem, err = ts.cacheEngine.LoadItem(ts.ctx, dbMetaInfo, bv.Item, opts)
				}))
			require.NoError(b, err)
			require.Equal(b, bv.BsKey, fetchedItem.Key)
		}
		b.Logf("load - from secondary index: %s\n", bd.String())
	}

	{ // list
		bd := testbench.New()
		pagination := &ltngenginemodels.Pagination{
			PageID:   1,
			PageSize: 50,
		}
		opts := &ltngenginemodels.IndexOpts{
			IndexProperties: ltngenginemodels.IndexProperties{
				ListSearchPattern: ltngenginemodels.All,
			},
		}
		var err error
		var fetchedItems *ltngenginemodels.ListItemsResult
		bd.CalcAvg(bd.CalcElapsed(func() {
			fetchedItems, err = ts.cacheEngine.ListItems(ts.ctx, dbMetaInfo, pagination, opts)
		}))
		assert.NoError(b, err)
		assert.Len(b, fetchedItems.Items, 50)
		fetchedItemsMap := ltngenginemodels.IndexListToMap(fetchedItems.Items)

		for _, user := range ts.users {
			bv := GetUserBytesValues(b, ts.testsuite, user)
			strKey := hex.EncodeToString(bv.Item.Key)
			_, ok := fetchedItemsMap[strKey]
			assert.True(b, ok)
		}

		b.Logf("list: %s\n", bd.String())
	}
}
