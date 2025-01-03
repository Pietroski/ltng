package memorystorev1

import (
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
