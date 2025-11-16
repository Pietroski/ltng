package v3

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"gitlab.com/pietroski-software-company/golang/devex/execx"
	"gitlab.com/pietroski-software-company/golang/devex/random"

	filequeuev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/file_queue/v1"
	v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdb/v3"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	fileiomodels "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/models"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
)

func TestLTNGEngineFlow(t *testing.T) {
	t.Run("store crud tests", func(t *testing.T) {
		t.Run("for single store", func(t *testing.T) {
			t.Run("standard", func(t *testing.T) {
				ctx := prepareTest(t)

				ltngEngine, err := New(ctx)
				require.NoError(t, err)

				info, err := ltngEngine.CreateStore(ctx, &v4.StoreInfo{
					Name: "test-store",
					Path: "test-path",
				})
				require.NoError(t, err)
				require.NotNil(t, info)
				t.Log(info)

				info, err = ltngEngine.LoadStore(ctx, &v4.StoreInfo{
					Name: "test-store",
					Path: "test-path",
				})
				require.NoError(t, err)
				t.Log(info)

				err = ltngEngine.DeleteStore(ctx, &v4.StoreInfo{
					Name: "test-store",
					Path: "test-path",
				})
				require.NoError(t, err)

				info, err = ltngEngine.LoadStore(ctx, &v4.StoreInfo{
					Name: "test-store",
					Path: "test-path",
				})
				require.Error(t, err)
				require.Nil(t, info)
				t.Log(info)

				ltngEngine.Close()
			})

			t.Run("detect last opened at difference after closing", func(t *testing.T) {
				t.Run("with close only", func(t *testing.T) {
					ctx := prepareTest(t)

					ltngEngine, err := New(ctx)
					require.NoError(t, err)
					infoHistory := make([]*v4.StoreInfo, 0)

					info, err := ltngEngine.CreateStore(ctx, &v4.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					info, err = ltngEngine.LoadStore(ctx, &v4.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					ltngEngine.Close()
					time.Sleep(time.Millisecond * 1_000)

					info, err = ltngEngine.LoadStore(ctx, &v4.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					infos, err := ltngEngine.ListStores(ctx, &ltngdata.Pagination{
						PageID:   1,
						PageSize: 5,
					})
					require.NoError(t, err)
					require.Len(t, infos, 1)
					t.Log(infos)

					for _, info = range infos {
						t.Log(info)
					}

					err = ltngEngine.DeleteStore(ctx, &v4.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)

					info, err = ltngEngine.LoadStore(ctx, &v4.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.Error(t, err)
					require.Nil(t, info)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					assertLOFromHistory(t, infoHistory)

					ltngEngine.Close()
				})

				t.Run("with restart", func(t *testing.T) {
					ctx := prepareTest(t)

					ltngEngine, err := New(ctx)
					require.NoError(t, err)
					infoHistory := make([]*v4.StoreInfo, 0)

					info, err := ltngEngine.CreateStore(ctx, &v4.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					info, err = ltngEngine.LoadStore(ctx, &v4.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					err = ltngEngine.Restart(ctx)
					require.NoError(t, err)
					time.Sleep(time.Millisecond * 1_000)

					info, err = ltngEngine.LoadStore(ctx, &v4.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					infos, err := ltngEngine.ListStores(ctx, &ltngdata.Pagination{
						PageID:   1,
						PageSize: 5,
					})
					require.NoError(t, err)
					require.Len(t, infos, 1)
					t.Log(infos)

					for _, info = range infos {
						t.Log(info)
					}

					err = ltngEngine.DeleteStore(ctx, &v4.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)

					info, err = ltngEngine.LoadStore(ctx, &v4.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.Error(t, err)
					require.Nil(t, info)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					assertLOFromHistory(t, infoHistory)

					ltngEngine.Close()
				})
			})
		})

		t.Run("for multiple stores", func(t *testing.T) {
			ctx := prepareTest(t)

			ltngEngine, err := New(ctx)
			require.NoError(t, err)

			info, err := ltngEngine.CreateStore(ctx, &v4.StoreInfo{
				Name: "test-store",
				Path: "test-path",
			})
			require.NoError(t, err)
			require.NotNil(t, info)

			info, err = ltngEngine.CreateStore(ctx, &v4.StoreInfo{
				Name: "test-another-store",
				Path: "test-another-path",
			})
			require.NoError(t, err)

			infos, err := ltngEngine.ListStores(ctx, &ltngdata.Pagination{
				PageID:   1,
				PageSize: 5,
			})
			require.NoError(t, err)
			require.Len(t, infos, 2)
			t.Log(infos)

			for _, info = range infos {
				t.Log(info)
			}

			err = ltngEngine.DeleteStore(ctx, &v4.StoreInfo{
				Name: "test-store",
				Path: "test-path",
			})
			require.NoError(t, err)

			err = ltngEngine.DeleteStore(ctx, &v4.StoreInfo{
				Name: "test-another-store",
				Path: "test-another-path",
			})
			require.NoError(t, err)

			infos, err = ltngEngine.ListStores(ctx, &ltngdata.Pagination{
				PageID:   1,
				PageSize: 5,
			})
			require.NoError(t, err)
			require.Len(t, infos, 0)
			t.Log(infos, err)

			info, err = ltngEngine.LoadStore(ctx, v4.DBManagerStoreInfo.RelationalInfo())
			require.NoError(t, err)
			require.NotNil(t, info)
			t.Log(info)

			ltngEngine.Close()
		})

		t.Run("for inexistent store", func(t *testing.T) {
			ctx := prepareTest(t)

			ltngEngine, err := New(ctx)
			require.NoError(t, err)

			err = ltngEngine.DeleteStore(ctx, &v4.StoreInfo{
				Name: "delete-inexistent-store",
				Path: "delete-inexistent-path",
			})
			require.Error(t, err)

			ltngEngine.Close()
		})
	})

	t.Run("item crud tests", func(t *testing.T) {
		t.Run("for single item", func(t *testing.T) {
			t.Run("standard", func(t *testing.T) {
				ts := initTestSuite(t)
				dbInfo := createTestStore(t, ts.ctx, ts)
				userData := generateTestUser(t)
				bsValues := getValues(t, ts, userData)
				databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()

				// #################################################################################### \\

				item := &v4.Item{
					Key:   bsValues.bsKey,
					Value: bsValues.bsValue,
				}
				createOpts := &v4.IndexOpts{
					HasIdx:       true,
					ParentKey:    item.Key,
					IndexingKeys: [][]byte{bsValues.bsKey, bsValues.secondaryIndexBs},
				}
				_, err := ts.ltngEngine.CreateItem(ts.ctx, databaseMetaInfo, item, createOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &v4.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &v4.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)
				}

				{
					// list items - default search
					items, err := ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				{
					deleteOpts := &v4.IndexOpts{
						HasIdx: true,
						IndexProperties: v4.IndexProperties{
							IndexDeletionBehaviour: v4.Cascade,
						},
					}
					_, err = ts.ltngEngine.DeleteItem(ts.ctx, databaseMetaInfo, item, deleteOpts)
					require.NoError(t, err)
				}

				{
					// search by key
					searchOpts := &v4.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by key - parent key
					searchOpts = &v4.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by index
					searchOpts = &v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)
				}

				{
					// list items - default search
					items, err := ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}
			})

			t.Run("delete index only", func(t *testing.T) {
				ts := initTestSuite(t)
				dbInfo := createTestStore(t, ts.ctx, ts)
				userData := generateTestUser(t)
				bsValues := getValues(t, ts, userData)
				databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()

				// #################################################################################### \\

				item := &v4.Item{
					Key:   bsValues.bsKey,
					Value: bsValues.bsValue,
				}
				createOpts := &v4.IndexOpts{
					HasIdx:       true,
					ParentKey:    item.Key,
					IndexingKeys: [][]byte{bsValues.bsKey, bsValues.secondaryIndexBs},
				}
				_, err := ts.ltngEngine.CreateItem(ts.ctx, databaseMetaInfo, item, createOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &v4.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &v4.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)
				}

				{
					// list items - default search
					items, err := ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				{
					deleteOpts := &v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
						IndexProperties: v4.IndexProperties{
							IndexDeletionBehaviour: v4.IndexOnly,
						},
					}
					_, err = ts.ltngEngine.DeleteItem(ts.ctx, databaseMetaInfo, item, deleteOpts)
					require.NoError(t, err)
				}

				{
					// search by key
					searchOpts := &v4.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					// search by key - parent key
					searchOpts = &v4.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					searchOpts = &v4.IndexOpts{
						HasIdx:       true,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
						IndexProperties: v4.IndexProperties{
							IndexSearchPattern: v4.AndComputational,
						},
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, nil, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)
				}

				{
					// list items - default search
					items, err := ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}
			})

			t.Run("upsert", func(t *testing.T) {
				ts := initTestSuite(t)
				dbInfo := createTestStore(t, ts.ctx, ts)
				userData := generateTestUser(t)
				bsValues := getValues(t, ts, userData)
				databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()

				// #################################################################################### \\

				item := &v4.Item{
					Key:   bsValues.bsKey,
					Value: bsValues.bsValue,
				}
				createOpts := &v4.IndexOpts{
					HasIdx:       true,
					ParentKey:    item.Key,
					IndexingKeys: [][]byte{bsValues.bsKey, bsValues.secondaryIndexBs},
				}
				_, err := ts.ltngEngine.CreateItem(ts.ctx, databaseMetaInfo, item, createOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &v4.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &v4.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)
				}

				{
					// list items - default search
					items, err := ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				item = &v4.Item{
					Key:   bsValues.bsKey,
					Value: bsValues.bsValue,
				}
				createOpts = &v4.IndexOpts{
					HasIdx:       true,
					ParentKey:    item.Key,
					IndexingKeys: [][]byte{bsValues.bsKey, bsValues.secondaryIndexBs, bsValues.extraUpsertIndex},
				}
				_, err = ts.ltngEngine.UpsertItem(ts.ctx, databaseMetaInfo, item, createOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &v4.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &v4.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.bsKey},
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.extraUpsertIndex},
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)
				}

				{
					// list items - default search
					items, err := ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				{
					deleteOpts := &v4.IndexOpts{
						HasIdx: true,
						IndexProperties: v4.IndexProperties{
							IndexDeletionBehaviour: v4.Cascade,
						},
					}
					_, err = ts.ltngEngine.DeleteItem(ts.ctx, databaseMetaInfo, item, deleteOpts)
					require.NoError(t, err)
				}

				{
					// search by key
					searchOpts := &v4.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by key - parent key
					searchOpts = &v4.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by index
					searchOpts = &v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)
				}

				{
					// list items - default search
					items, err := ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}
			})

			t.Run("detect last opened at difference after closing", func(t *testing.T) {
				ts := initTestSuite(t)
				dbInfo := createTestStore(t, ts.ctx, ts)
				userData := generateTestUser(t)
				bsValues := getValues(t, ts, userData)
				databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()

				// #################################################################################### \\

				item := &v4.Item{
					Key:   bsValues.bsKey,
					Value: bsValues.bsValue,
				}
				createOpts := &v4.IndexOpts{
					HasIdx:       true,
					ParentKey:    item.Key,
					IndexingKeys: [][]byte{bsValues.bsKey, bsValues.secondaryIndexBs},
				}
				_, err := ts.ltngEngine.CreateItem(ts.ctx, databaseMetaInfo, item, createOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &v4.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &v4.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)
				}

				{
					// list items - default search
					items, err := ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				err = ts.ltngEngine.Restart(ts.ctx)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &v4.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &v4.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)
				}

				{
					// list items - default search
					items, err := ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				deleteOpts := &v4.IndexOpts{
					HasIdx: true,
					IndexProperties: v4.IndexProperties{
						IndexDeletionBehaviour: v4.Cascade,
					},
				}
				_, err = ts.ltngEngine.DeleteItem(ts.ctx, databaseMetaInfo, item, deleteOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &v4.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by key - parent key
					searchOpts = &v4.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by index
					searchOpts = &v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)
				}

				{
					// list items - default search
					items, err := ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}
			})

			t.Run("process and close", func(t *testing.T) {
				ts := initTestSuite(t)
				dbInfo := createTestStore(t, ts.ctx, ts)
				userData := generateTestUser(t)
				bsValues := getValues(t, ts, userData)
				databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()

				// #################################################################################### \\

				item := &v4.Item{
					Key:   bsValues.bsKey,
					Value: bsValues.bsValue,
				}
				createOpts := &v4.IndexOpts{
					HasIdx:       true,
					ParentKey:    item.Key,
					IndexingKeys: [][]byte{bsValues.bsKey, bsValues.secondaryIndexBs},
				}
				_, err := ts.ltngEngine.CreateItem(ts.ctx, databaseMetaInfo, item, createOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &v4.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &v4.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)
				}

				{
					// list items - default search
					items, err := ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				deleteOpts := &v4.IndexOpts{
					HasIdx: true,
					IndexProperties: v4.IndexProperties{
						IndexDeletionBehaviour: v4.Cascade,
					},
				}
				_, err = ts.ltngEngine.DeleteItem(ts.ctx, databaseMetaInfo, item, deleteOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &v4.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by key - parent key
					searchOpts = &v4.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by index
					searchOpts = &v4.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)
				}

				{
					// list items - default search
					items, err := ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.ltngEngine.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&v4.IndexOpts{
							IndexProperties: v4.IndexProperties{
								ListSearchPattern: v4.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}
			})
		})

		t.Run("for multiple items", func(t *testing.T) {
			ts := initTestSuite(t)
			dbInfo := createTestStore(t, ts.ctx, ts)
			userCount := 10
			userList := generateTestUsers(t, userCount)
			databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()

			// create ops
			for _, userData := range userList {

				t.Log(userData.Email)

				bvs := getValues(t, ts, userData)

				createOpts := &v4.IndexOpts{
					HasIdx:       true,
					ParentKey:    bvs.item.Key,
					IndexingKeys: [][]byte{bvs.bsKey, bvs.secondaryIndexBs},
				}
				_, err := ts.ltngEngine.CreateItem(ts.ctx, databaseMetaInfo, bvs.item, createOpts)
				require.NoError(t, err)
			}

			// list ops
			{
				// list items - default search
				items, err := ts.ltngEngine.ListItems(
					ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
					&v4.IndexOpts{
						IndexProperties: v4.IndexProperties{
							ListSearchPattern: v4.Default,
						},
					},
				)
				require.NoError(t, err)
				require.Len(t, items.Items, userCount)

				for _, item := range items.Items {
					t.Log(string(item.Key), string(item.Value))
				}

				// list items - search for all
				items, err = ts.ltngEngine.ListItems(
					ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
					&v4.IndexOpts{
						IndexProperties: v4.IndexProperties{
							ListSearchPattern: v4.All,
						},
					},
				)
				require.NoError(t, err)
				require.Len(t, items.Items, userCount)

				for _, item := range items.Items {
					t.Log(string(item.Key), string(item.Value))
				}
			}

			// search one by one
			for _, userData := range userList {
				bvs := getValues(t, ts, userData)

				// search by key
				searchOpts := &v4.IndexOpts{}
				loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.NoError(t, err)
				require.NotNil(t, loadedItem)

				var u user
				err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
				require.NoError(t, err)
				t.Log(u)

				// search by key - parent key
				searchOpts = &v4.IndexOpts{
					HasIdx:    true,
					ParentKey: bvs.item.Key,
				}
				loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.NoError(t, err)
				require.NotNil(t, loadedItem)

				err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
				require.NoError(t, err)
				t.Log(u)

				// search by index
				searchOpts = &v4.IndexOpts{
					HasIdx:       true,
					ParentKey:    bvs.item.Key,
					IndexingKeys: [][]byte{bvs.secondaryIndexBs},
				}
				loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.NoError(t, err)
				require.NotNil(t, loadedItem)

				err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
				require.NoError(t, err)
				t.Log(u)
			}

			// delete and search
			for _, userData := range userList {
				bvs := getValues(t, ts, userData)

				deleteOpts := &v4.IndexOpts{
					HasIdx: true,
					IndexProperties: v4.IndexProperties{
						IndexDeletionBehaviour: v4.Cascade,
					},
				}
				_, err := ts.ltngEngine.DeleteItem(ts.ctx, databaseMetaInfo, bvs.item, deleteOpts)
				require.NoError(t, err)

				// search by key
				searchOpts := &v4.IndexOpts{}
				loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				// t.Logf("key %s - value %s - err: %v", loadedItem.Key, loadedItem.Value, err)
				require.Error(t, err)
				require.Nil(t, loadedItem)

				// search by key - parent key
				searchOpts = &v4.IndexOpts{
					HasIdx:    true,
					ParentKey: bvs.item.Key,
				}
				loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.Error(t, err)
				require.Nil(t, loadedItem)

				// search by index
				searchOpts = &v4.IndexOpts{
					HasIdx:       true,
					ParentKey:    bvs.item.Key,
					IndexingKeys: [][]byte{bvs.secondaryIndexBs},
				}
				loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.Error(t, err)
				require.Nil(t, loadedItem)
			}

			// list ops
			{
				// list items - default search
				items, err := ts.ltngEngine.ListItems(
					ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
					&v4.IndexOpts{
						IndexProperties: v4.IndexProperties{
							ListSearchPattern: v4.Default,
						},
					},
				)
				require.NoError(t, err)
				require.Len(t, items.Items, 0)

				for _, item := range items.Items {
					t.Log(string(item.Key), string(item.Value))
				}

				// list items - search for all
				items, err = ts.ltngEngine.ListItems(
					ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
					&v4.IndexOpts{
						IndexProperties: v4.IndexProperties{
							ListSearchPattern: v4.All,
						},
					},
				)
				require.NoError(t, err)
				require.Len(t, items.Items, 0)

				for _, item := range items.Items {
					t.Log(string(item.Key), string(item.Value))
				}
			}
		})

		t.Run("for inexistent item", func(t *testing.T) {
			ts := initTestSuite(t)
			dbInfo := createTestStore(t, ts.ctx, ts)
			userData := generateTestUser(t)
			bvs := getValues(t, ts, userData)
			databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()

			// #################################################################################### \\

			{
				// search by key
				searchOpts := &v4.IndexOpts{}
				loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.Error(t, err)
				require.Nil(t, loadedItem)

				// search by key - parent key
				searchOpts = &v4.IndexOpts{
					HasIdx:    true,
					ParentKey: bvs.item.Key,
				}
				loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.Error(t, err)
				require.Nil(t, loadedItem)

				// search by index
				searchOpts = &v4.IndexOpts{
					HasIdx:       true,
					ParentKey:    bvs.item.Key,
					IndexingKeys: [][]byte{bvs.secondaryIndexBs},
				}
				loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.Error(t, err)
				require.Nil(t, loadedItem)
			}

			{
				// list items - default search
				items, err := ts.ltngEngine.ListItems(
					ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
					&v4.IndexOpts{
						IndexProperties: v4.IndexProperties{
							ListSearchPattern: v4.Default,
						},
					},
				)
				require.NoError(t, err)
				require.Len(t, items.Items, 0)

				for _, item := range items.Items {
					t.Log(string(item.Key), string(item.Value))
				}

				// list items - search for all
				items, err = ts.ltngEngine.ListItems(
					ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
					&v4.IndexOpts{
						IndexProperties: v4.IndexProperties{
							ListSearchPattern: v4.All,
						},
					},
				)
				require.NoError(t, err)
				require.Len(t, items.Items, 0)

				for _, item := range items.Items {
					t.Log(string(item.Key), string(item.Value))
				}
			}
		})
	})

	t.Run("test on and off", func(t *testing.T) {
		ts := initTestSuite(t)
		dbInfo := createTestStore(t, ts.ctx, ts)
		_ = dbInfo
		_ = ts
	})
}

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
		require.NoError(t, err)

		counter++
	}
	t.Log(counter)
}

func TestCheckFileCount(t *testing.T) {
	err := execx.Run("sh", "-c",
		"find .ltngdb/v1/stores/test-path -maxdepth 1 -type f | wc -l",
	)
	require.NoError(t, err)
}

func prepareTest(t *testing.T) context.Context {
	ctx := context.Background()
	err := osx.DelHard(ctx, fileiomodels.FileQueueBasePath)
	err = osx.DelHard(ctx, v4.DBBasePath)
	require.NoError(t, err)

	return ctx
}

func prepareTestWithCancel(t *testing.T) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	err := osx.DelHard(ctx, fileiomodels.FileQueueBasePath)
	err = osx.DelHard(ctx, v4.DBBasePath)
	require.NoError(t, err)

	return ctx, cancel
}

func assertLOFromHistory(t *testing.T, history []*v4.StoreInfo) {
	previous := history[0]
	cutHistory := history[1:]
	var differentLastOpenedCheck bool
	for _, info := range cutHistory {
		if info == nil {
			continue
		}

		if info.LastOpenedAt != previous.LastOpenedAt {
			differentLastOpenedCheck = true
		}
	}
	require.True(t, differentLastOpenedCheck)
}

type (
	user struct {
		UUID      string
		Username  string
		Password  string
		Email     string
		Name      string
		Surname   string
		Age       uint8
		CreatedAt int64
		UpdatedAt int64
	}
)

type (
	testSuite struct {
		ctx        context.Context
		ltngEngine *LTNGEngine
	}
)

type bytesValues struct {
	bsKey, bsValue, secondaryIndexBs, extraUpsertIndex []byte
	item                                               *v4.Item
}

func initTestSuite(t *testing.T) *testSuite {
	ctx := prepareTest(t)
	ltngEngine, err := New(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		ltngEngine.Close()
	})

	return &testSuite{
		ctx:        ctx,
		ltngEngine: ltngEngine,
	}
}

func setUserTime(userData *user) {
	tn := time.Now().UTC().Unix()
	userData.CreatedAt = tn
	userData.UpdatedAt = tn
}

func getValues(t *testing.T, ts *testSuite, userData *user) *bytesValues {
	setUserTime(userData)

	bsKey, err := ts.ltngEngine.serializer.Serialize(userData.Email)
	require.NoError(t, err)
	require.NotNil(t, bsKey)
	t.Log(string(bsKey))

	bsValue, err := ts.ltngEngine.serializer.Serialize(userData)
	require.NoError(t, err)
	require.NotNil(t, bsValue)
	t.Log(string(bsValue))

	secondaryIndexBs, err := ts.ltngEngine.serializer.Serialize(userData.Username)
	require.NoError(t, err)
	require.NotNil(t, secondaryIndexBs)
	t.Log(string(secondaryIndexBs))

	extraUpsertIndexBs, err := ts.ltngEngine.serializer.Serialize(userData.UUID)
	require.NoError(t, err)
	require.NotNil(t, extraUpsertIndexBs)
	t.Log(string(extraUpsertIndexBs))

	return &bytesValues{
		bsKey:            bsKey,
		bsValue:          bsValue,
		secondaryIndexBs: secondaryIndexBs,
		extraUpsertIndex: extraUpsertIndexBs,
		item: &v4.Item{
			Key:   bsKey,
			Value: bsValue,
		},
	}
}

func createTestStore(t *testing.T, ctx context.Context, ts *testSuite) *v4.StoreInfo {
	dbInfo := &v4.StoreInfo{
		Name: "test-store",
		Path: "test-path",
	}
	info, err := ts.ltngEngine.CreateStore(ctx, dbInfo)
	require.NoError(t, err)
	require.NotNil(t, info)

	info, err = ts.ltngEngine.LoadStore(ctx, dbInfo)
	require.NoError(t, err)

	infos, err := ts.ltngEngine.ListStores(ctx, &ltngdata.Pagination{
		PageID:   1,
		PageSize: 5,
	})
	require.NoError(t, err)
	require.Len(t, infos, 1)
	t.Log(infos)

	for _, info = range infos {
		t.Log(info)
	}

	return info
}

func generateTestUser(t *testing.T) *user {
	newUUID, err := uuid.NewUUID()
	require.NoError(t, err)

	timeNow := time.Now().UTC().Unix()
	userData := &user{
		UUID:      newUUID.String(),
		Username:  random.StringWithPrefixWithSep(12, "username", "-"),
		Password:  random.StringWithPrefixWithSep(12, "password", "-"),
		Email:     random.Email(),
		Name:      random.StringWithPrefixWithSep(12, "name", "-"),
		Surname:   random.StringWithPrefixWithSep(12, "surname", "-"),
		Age:       uint8(random.Int(0, math.MaxUint8)),
		CreatedAt: timeNow,
		UpdatedAt: timeNow,
	}
	t.Log(userData.Email)

	return userData
}

func generateTestUsers(t *testing.T, n int) []*user {
	users := make([]*user, n)
	for i := 0; i < n; i++ {
		users[i] = generateTestUser(t)
	}

	return users
}
