package v2

import (
	"context"
	"math"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	go_random "gitlab.com/pietroski-software-company/tools/random/go-random/pkg/tools/random"

	filequeuev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/file_queue/v1"
	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/execx"
)

func TestLTNGEngineFlow(t *testing.T) {
	t.Run("store crud tests", func(t *testing.T) {
		t.Run("for single store", func(t *testing.T) {
			t.Run("standard", func(t *testing.T) {
				ctx := prepareTest(t)

				ltngEngine, err := New(ctx)
				require.NoError(t, err)

				info, err := ltngEngine.CreateStore(ctx, &ltngenginemodels.StoreInfo{
					Name: "test-store",
					Path: "test-path",
				})
				require.NoError(t, err)
				require.NotNil(t, info)
				t.Log(info)

				info, err = ltngEngine.LoadStore(ctx, &ltngenginemodels.StoreInfo{
					Name: "test-store",
					Path: "test-path",
				})
				require.NoError(t, err)
				t.Log(info)

				err = ltngEngine.DeleteStore(ctx, &ltngenginemodels.StoreInfo{
					Name: "test-store",
					Path: "test-path",
				})
				require.NoError(t, err)

				info, err = ltngEngine.LoadStore(ctx, &ltngenginemodels.StoreInfo{
					Name: "test-store",
					Path: "test-path",
				})
				require.Error(t, err)
				require.Nil(t, info)
				t.Log(info)
			})

			t.Run("detect last opened at difference after closing", func(t *testing.T) {
				t.Run("with close only", func(t *testing.T) {
					ctx := prepareTest(t)

					ltngEngine, err := New(ctx)
					require.NoError(t, err)
					infoHistory := make([]*ltngenginemodels.StoreInfo, 0)

					info, err := ltngEngine.CreateStore(ctx, &ltngenginemodels.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					info, err = ltngEngine.LoadStore(ctx, &ltngenginemodels.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					ltngEngine.Close()
					time.Sleep(time.Millisecond * 1_000)

					info, err = ltngEngine.LoadStore(ctx, &ltngenginemodels.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					infos, err := ltngEngine.ListStores(ctx, &ltngenginemodels.Pagination{
						PageID:   1,
						PageSize: 5,
					})
					require.NoError(t, err)
					require.Len(t, infos, 1)
					t.Log(infos)

					for _, info = range infos {
						t.Log(info)
					}

					err = ltngEngine.DeleteStore(ctx, &ltngenginemodels.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)

					info, err = ltngEngine.LoadStore(ctx, &ltngenginemodels.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.Error(t, err)
					require.Nil(t, info)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					assertLOFromHistory(t, infoHistory)
				})

				t.Run("with restart", func(t *testing.T) {
					ctx := prepareTest(t)

					ltngEngine, err := New(ctx)
					require.NoError(t, err)
					infoHistory := make([]*ltngenginemodels.StoreInfo, 0)

					info, err := ltngEngine.CreateStore(ctx, &ltngenginemodels.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					info, err = ltngEngine.LoadStore(ctx, &ltngenginemodels.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					err = ltngEngine.Restart(ctx)
					require.NoError(t, err)
					time.Sleep(time.Millisecond * 1_000)

					info, err = ltngEngine.LoadStore(ctx, &ltngenginemodels.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					infos, err := ltngEngine.ListStores(ctx, &ltngenginemodels.Pagination{
						PageID:   1,
						PageSize: 5,
					})
					require.NoError(t, err)
					require.Len(t, infos, 1)
					t.Log(infos)

					for _, info = range infos {
						t.Log(info)
					}

					err = ltngEngine.DeleteStore(ctx, &ltngenginemodels.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)

					info, err = ltngEngine.LoadStore(ctx, &ltngenginemodels.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.Error(t, err)
					require.Nil(t, info)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					assertLOFromHistory(t, infoHistory)
				})
			})
		})

		t.Run("for multiple stores", func(t *testing.T) {
			ctx := prepareTest(t)

			ltngEngine, err := New(ctx)
			require.NoError(t, err)

			info, err := ltngEngine.CreateStore(ctx, &ltngenginemodels.StoreInfo{
				Name: "test-store",
				Path: "test-path",
			})
			require.NoError(t, err)
			require.NotNil(t, info)

			info, err = ltngEngine.CreateStore(ctx, &ltngenginemodels.StoreInfo{
				Name: "test-another-store",
				Path: "test-another-path",
			})
			require.NoError(t, err)

			infos, err := ltngEngine.ListStores(ctx, &ltngenginemodels.Pagination{
				PageID:   1,
				PageSize: 5,
			})
			require.NoError(t, err)
			require.Len(t, infos, 2)
			t.Log(infos)

			for _, info = range infos {
				t.Log(info)
			}

			err = ltngEngine.DeleteStore(ctx, &ltngenginemodels.StoreInfo{
				Name: "test-store",
				Path: "test-path",
			})
			require.NoError(t, err)

			err = ltngEngine.DeleteStore(ctx, &ltngenginemodels.StoreInfo{
				Name: "test-another-store",
				Path: "test-another-path",
			})
			require.NoError(t, err)

			infos, err = ltngEngine.ListStores(ctx, &ltngenginemodels.Pagination{
				PageID:   1,
				PageSize: 5,
			})
			require.NoError(t, err)
			require.Len(t, infos, 0)
			t.Log(infos, err)

			info, err = ltngEngine.LoadStore(ctx, ltngenginemodels.DBManagerStoreInfo.RelationalInfo())
			require.NoError(t, err)
			require.NotNil(t, info)
			t.Log(info)
		})

		t.Run("for inexistent store", func(t *testing.T) {
			ctx := prepareTest(t)

			ltngEngine, err := New(ctx)
			require.NoError(t, err)

			err = ltngEngine.DeleteStore(ctx, &ltngenginemodels.StoreInfo{
				Name: "delete-inexistent-store",
				Path: "delete-inexistent-path",
			})
			require.Error(t, err)
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

				item := &ltngenginemodels.Item{
					Key:   bsValues.bsKey,
					Value: bsValues.bsValue,
				}
				createOpts := &ltngenginemodels.IndexOpts{
					HasIdx:       true,
					ParentKey:    item.Key,
					IndexingKeys: [][]byte{bsValues.bsKey, bsValues.secondaryIndexBs},
				}
				_, err := ts.ltngEngine.CreateItem(ts.ctx, databaseMetaInfo, item, createOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &ltngenginemodels.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &ltngenginemodels.IndexOpts{
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
					searchOpts = &ltngenginemodels.IndexOpts{
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.Default,
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.All,
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
					deleteOpts := &ltngenginemodels.IndexOpts{
						HasIdx: true,
						IndexProperties: ltngenginemodels.IndexProperties{
							IndexDeletionBehaviour: ltngenginemodels.Cascade,
						},
					}
					_, err = ts.ltngEngine.DeleteItem(ts.ctx, databaseMetaInfo, item, deleteOpts)
					require.NoError(t, err)
				}

				{
					// search by key
					searchOpts := &ltngenginemodels.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by key - parent key
					searchOpts = &ltngenginemodels.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by index
					searchOpts = &ltngenginemodels.IndexOpts{
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.Default,
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.All,
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

				item := &ltngenginemodels.Item{
					Key:   bsValues.bsKey,
					Value: bsValues.bsValue,
				}
				createOpts := &ltngenginemodels.IndexOpts{
					HasIdx:       true,
					ParentKey:    item.Key,
					IndexingKeys: [][]byte{bsValues.bsKey, bsValues.secondaryIndexBs},
				}
				_, err := ts.ltngEngine.CreateItem(ts.ctx, databaseMetaInfo, item, createOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &ltngenginemodels.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &ltngenginemodels.IndexOpts{
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
					searchOpts = &ltngenginemodels.IndexOpts{
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.Default,
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.All,
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
					searchOpts := &ltngenginemodels.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &ltngenginemodels.IndexOpts{
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
					searchOpts = &ltngenginemodels.IndexOpts{
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.Default,
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				deleteOpts := &ltngenginemodels.IndexOpts{
					HasIdx: true,
					IndexProperties: ltngenginemodels.IndexProperties{
						IndexDeletionBehaviour: ltngenginemodels.Cascade,
					},
				}
				_, err = ts.ltngEngine.DeleteItem(ts.ctx, databaseMetaInfo, item, deleteOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &ltngenginemodels.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by key - parent key
					searchOpts = &ltngenginemodels.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by index
					searchOpts = &ltngenginemodels.IndexOpts{
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.Default,
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.All,
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

				item := &ltngenginemodels.Item{
					Key:   bsValues.bsKey,
					Value: bsValues.bsValue,
				}
				createOpts := &ltngenginemodels.IndexOpts{
					HasIdx:       true,
					ParentKey:    item.Key,
					IndexingKeys: [][]byte{bsValues.bsKey, bsValues.secondaryIndexBs},
				}
				_, err := ts.ltngEngine.CreateItem(ts.ctx, databaseMetaInfo, item, createOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &ltngenginemodels.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &ltngenginemodels.IndexOpts{
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
					searchOpts = &ltngenginemodels.IndexOpts{
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.Default,
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				deleteOpts := &ltngenginemodels.IndexOpts{
					HasIdx: true,
					IndexProperties: ltngenginemodels.IndexProperties{
						IndexDeletionBehaviour: ltngenginemodels.Cascade,
					},
				}
				_, err = ts.ltngEngine.DeleteItem(ts.ctx, databaseMetaInfo, item, deleteOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &ltngenginemodels.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by key - parent key
					searchOpts = &ltngenginemodels.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by index
					searchOpts = &ltngenginemodels.IndexOpts{
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.Default,
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.All,
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
			t.Run("for multiple items new engine", func(t *testing.T) {
				ts := initTestSuite(t)
				dbInfo := createTestStore(t, ts.ctx, ts)
				userCount := 10
				userList := generateTestUsers(t, userCount)
				databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()

				// create ops
				for _, userData := range userList {

					t.Log(userData.Email)

					bvs := getValues(t, ts, userData)

					createOpts := &ltngenginemodels.IndexOpts{
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.Default,
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.All,
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
					searchOpts := &ltngenginemodels.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &ltngenginemodels.IndexOpts{
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
					searchOpts = &ltngenginemodels.IndexOpts{
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

					deleteOpts := &ltngenginemodels.IndexOpts{
						HasIdx: true,
						IndexProperties: ltngenginemodels.IndexProperties{
							IndexDeletionBehaviour: ltngenginemodels.Cascade,
						},
					}
					_, err := ts.ltngEngine.DeleteItem(ts.ctx, databaseMetaInfo, bvs.item, deleteOpts)
					require.NoError(t, err)

					// search by key
					searchOpts := &ltngenginemodels.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
					// t.Logf("key %s - value %s - err: %v", loadedItem.Key, loadedItem.Value, err)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by key - parent key
					searchOpts = &ltngenginemodels.IndexOpts{
						HasIdx:    true,
						ParentKey: bvs.item.Key,
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by index
					searchOpts = &ltngenginemodels.IndexOpts{
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.Default,
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.All,
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

			t.Run("for multiple items", func(t *testing.T) {
				ts := initTestSuite(t)
				dbInfo := createTestStore(t, ts.ctx, ts)
				userCount := 10
				userList := generateTestUsers(t, userCount)
				databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()

				for _, userData := range userList {
					t.Log(userData.Email)
					bvs := getValues(t, ts, userData)

					createOpts := &ltngenginemodels.IndexOpts{
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.Default,
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.All,
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
					searchOpts := &ltngenginemodels.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &ltngenginemodels.IndexOpts{
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
					searchOpts = &ltngenginemodels.IndexOpts{
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

					deleteOpts := &ltngenginemodels.IndexOpts{
						HasIdx: true,
						IndexProperties: ltngenginemodels.IndexProperties{
							IndexDeletionBehaviour: ltngenginemodels.Cascade,
						},
					}
					_, err := ts.ltngEngine.DeleteItem(ts.ctx, databaseMetaInfo, bvs.item, deleteOpts)
					require.NoError(t, err)

					// search by key
					searchOpts := &ltngenginemodels.IndexOpts{}
					loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by key - parent key
					searchOpts = &ltngenginemodels.IndexOpts{
						HasIdx:    true,
						ParentKey: bvs.item.Key,
					}
					loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by index
					searchOpts = &ltngenginemodels.IndexOpts{
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.Default,
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
						ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
						&ltngenginemodels.IndexOpts{
							IndexProperties: ltngenginemodels.IndexProperties{
								ListSearchPattern: ltngenginemodels.All,
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

		t.Run("for inexistent item", func(t *testing.T) {
			ts := initTestSuite(t)
			dbInfo := createTestStore(t, ts.ctx, ts)
			userData := generateTestUser(t)
			bvs := getValues(t, ts, userData)
			databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()

			// #################################################################################### \\

			{
				// search by key
				searchOpts := &ltngenginemodels.IndexOpts{}
				loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.Error(t, err)
				require.Nil(t, loadedItem)

				// search by key - parent key
				searchOpts = &ltngenginemodels.IndexOpts{
					HasIdx:    true,
					ParentKey: bvs.item.Key,
				}
				loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.Error(t, err)
				require.Nil(t, loadedItem)

				// search by index
				searchOpts = &ltngenginemodels.IndexOpts{
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
					ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
					&ltngenginemodels.IndexOpts{
						IndexProperties: ltngenginemodels.IndexProperties{
							ListSearchPattern: ltngenginemodels.Default,
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
					ts.ctx, databaseMetaInfo, ltngenginemodels.PageDefault(1),
					&ltngenginemodels.IndexOpts{
						IndexProperties: ltngenginemodels.IndexProperties{
							ListSearchPattern: ltngenginemodels.All,
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
	bs, err := execx.Executor(exec.Command(
		"sh", "-c",
		"find .ltngdb/v1/stores/test-path -maxdepth 1 -type f | wc -l",
	))
	require.NoError(t, err)
	t.Log(strings.TrimSpace(string(bs)))
}

func prepareTest(t *testing.T) context.Context {
	ctx := context.Background()
	_, err := execx.DelHardExec(ctx, ltngenginemodels.FQBasePath)
	_, err = execx.DelHardExec(ctx, ltngenginemodels.DBBasePath)
	require.NoError(t, err)

	return ctx
}

func assertLOFromHistory(t *testing.T, history []*ltngenginemodels.StoreInfo) {
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
	bsKey, bsValue, secondaryIndexBs []byte
	item                             *ltngenginemodels.Item
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

	return &bytesValues{
		bsKey:            bsKey,
		bsValue:          bsValue,
		secondaryIndexBs: secondaryIndexBs,
		item: &ltngenginemodels.Item{
			Key:   bsKey,
			Value: bsValue,
		},
	}
}

func createTestStore(t *testing.T, ctx context.Context, ts *testSuite) *ltngenginemodels.StoreInfo {
	dbInfo := &ltngenginemodels.StoreInfo{
		Name: "test-store",
		Path: "test-path",
	}
	info, err := ts.ltngEngine.CreateStore(ctx, dbInfo)
	require.NoError(t, err)
	require.NotNil(t, info)

	info, err = ts.ltngEngine.LoadStore(ctx, dbInfo)
	require.NoError(t, err)

	infos, err := ts.ltngEngine.ListStores(ctx, &ltngenginemodels.Pagination{
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
	timeNow := time.Now().UTC().Unix()
	userData := &user{
		Username:  go_random.RandomStringWithPrefixWithSep(12, "username", "-"),
		Password:  go_random.RandomStringWithPrefixWithSep(12, "password", "-"),
		Email:     go_random.RandomEmail(),
		Name:      go_random.RandomStringWithPrefixWithSep(12, "name", "-"),
		Surname:   go_random.RandomStringWithPrefixWithSep(12, "surname", "-"),
		Age:       uint8(go_random.RandomInt(0, math.MaxUint8)),
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
