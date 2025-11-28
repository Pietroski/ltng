package ltngdbenginev3

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"gitlab.com/pietroski-software-company/golang/devex/execx"
	"gitlab.com/pietroski-software-company/golang/devex/random"

	ltngdbenginemodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/mmap"
	fileiomodels "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/models"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
)

func TestLTNGEngineFlow(t *testing.T) {
	t.Run("store crud tests", func(t *testing.T) {
		t.Run("for single store", func(t *testing.T) {
			t.Run("standard", func(t *testing.T) {
				ctx := prepareTest(t)

				e, err := New(ctx)
				require.NoError(t, err)

				info, err := e.CreateStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
					Name: "test-store",
					Path: "test-path",
				})
				require.NoError(t, err)
				require.NotNil(t, info)
				t.Log(info)

				info, err = e.LoadStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
					Name: "test-store",
					Path: "test-path",
				})
				require.NoError(t, err)
				t.Log(info)

				err = e.DeleteStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
					Name: "test-store",
					Path: "test-path",
				})
				require.NoError(t, err)

				info, err = e.LoadStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
					Name: "test-store",
					Path: "test-path",
				})
				require.Error(t, err)
				require.Nil(t, info)
				t.Log(info)

				e.Close()
			})

			t.Run("detect last opened at difference after closing", func(t *testing.T) {
				t.Run("with close only", func(t *testing.T) {
					ctx := prepareTest(t)

					e, err := New(ctx)
					require.NoError(t, err)
					infoHistory := make([]*ltngdbenginemodelsv3.StoreInfo, 0)

					info, err := e.CreateStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					info, err = e.LoadStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					e.Close()
					time.Sleep(time.Millisecond * 1_000)

					info, err = e.LoadStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					infos, err := e.ListStores(ctx, &ltngdata.Pagination{
						PageID:   1,
						PageSize: 5,
					})
					require.NoError(t, err)
					require.Len(t, infos, 1)
					t.Log(infos)

					for _, info = range infos {
						t.Log(info)
					}

					err = e.DeleteStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)

					info, err = e.LoadStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.Error(t, err)
					require.Nil(t, info)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					assertLOFromHistory(t, infoHistory)

					e.Close()
				})

				t.Run("with restart", func(t *testing.T) {
					ctx := prepareTest(t)

					e, err := New(ctx)
					require.NoError(t, err)
					infoHistory := make([]*ltngdbenginemodelsv3.StoreInfo, 0)

					info, err := e.CreateStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					info, err = e.LoadStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					err = e.Restart(ctx)
					require.NoError(t, err)
					time.Sleep(time.Millisecond * 1_000)

					info, err = e.LoadStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					infos, err := e.ListStores(ctx, &ltngdata.Pagination{
						PageID:   1,
						PageSize: 5,
					})
					require.NoError(t, err)
					require.Len(t, infos, 1)
					t.Log(infos)

					for _, info = range infos {
						t.Log(info)
					}

					err = e.DeleteStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.NoError(t, err)

					info, err = e.LoadStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					require.Error(t, err)
					require.Nil(t, info)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					assertLOFromHistory(t, infoHistory)

					e.Close()
				})
			})
		})

		t.Run("for multiple stores", func(t *testing.T) {
			ctx := prepareTest(t)

			e, err := New(ctx)
			require.NoError(t, err)

			info, err := e.CreateStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
				Name: "test-store",
				Path: "test-path",
			})
			require.NoError(t, err)
			require.NotNil(t, info)

			info, err = e.CreateStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
				Name: "test-another-store",
				Path: "test-another-path",
			})
			require.NoError(t, err)

			infos, err := e.ListStores(ctx, &ltngdata.Pagination{
				PageID:   1,
				PageSize: 5,
			})
			t.Logf("infos: %+v", infos[0])
			t.Logf("infos: %+v", infos[1])
			require.NoError(t, err)
			require.Len(t, infos, 2)
			t.Log(infos)

			for _, info = range infos {
				t.Log(info)
			}

			err = e.DeleteStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
				Name: "test-store",
				Path: "test-path",
			})
			require.NoError(t, err)

			err = e.DeleteStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
				Name: "test-another-store",
				Path: "test-another-path",
			})
			require.NoError(t, err)

			infos, err = e.ListStores(ctx, &ltngdata.Pagination{
				PageID:   1,
				PageSize: 5,
			})
			require.NoError(t, err)
			require.Len(t, infos, 0)
			t.Log(infos, err)

			info, err = e.LoadStore(ctx, ltngdbenginemodelsv3.DBManagerStoreInfo)
			require.NoError(t, err)
			require.NotNil(t, info)
			t.Log(info)

			e.Close()
		})

		t.Run("for inexistent store", func(t *testing.T) {
			ctx := prepareTest(t)

			e, err := New(ctx)
			require.NoError(t, err)

			err = e.DeleteStore(ctx, &ltngdbenginemodelsv3.StoreInfo{
				Name: "delete-inexistent-store",
				Path: "delete-inexistent-path",
			})
			require.Error(t, err)

			e.Close()
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

				item := &ltngdbenginemodelsv3.Item{
					Key:   bsValues.bsKey,
					Value: bsValues.bsValue,
				}
				createOpts := &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:       true,
					ParentKey:    item.Key,
					IndexingKeys: [][]byte{bsValues.bsKey, bsValues.secondaryIndexBs},
				}
				_, err := ts.e.CreateItem(ts.ctx, databaseMetaInfo, item, createOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &ltngdbenginemodelsv3.IndexOpts{}
					loadedItem, err := ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)
				}

				{
					// list items - default search
					items, err := ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.All,
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
					deleteOpts := &ltngdbenginemodelsv3.IndexOpts{
						HasIdx: true,
						IndexProperties: ltngdbenginemodelsv3.IndexProperties{
							IndexDeletionBehaviour: ltngdbenginemodelsv3.Cascade,
						},
					}
					_, err = ts.e.DeleteItem(ts.ctx, databaseMetaInfo, item, deleteOpts)
					require.NoError(t, err)
				}

				{
					// search by key
					searchOpts := &ltngdbenginemodelsv3.IndexOpts{}
					loadedItem, err := ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by key - parent key
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by index
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)
				}

				{
					// list items - default search
					items, err := ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				ts.e.Close()
			})

			t.Run("delete index only", func(t *testing.T) {
				ts := initTestSuite(t)
				dbInfo := createTestStore(t, ts.ctx, ts)
				userData := generateTestUser(t)
				bsValues := getValues(t, ts, userData)
				databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()

				// #################################################################################### \\

				item := &ltngdbenginemodelsv3.Item{
					Key:   bsValues.bsKey,
					Value: bsValues.bsValue,
				}
				createOpts := &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:       true,
					ParentKey:    item.Key,
					IndexingKeys: [][]byte{bsValues.bsKey, bsValues.secondaryIndexBs},
				}
				_, err := ts.e.CreateItem(ts.ctx, databaseMetaInfo, item, createOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &ltngdbenginemodelsv3.IndexOpts{}
					loadedItem, err := ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)
				}

				{
					// list items - default search
					items, err := ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.All,
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
					deleteOpts := &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
						IndexProperties: ltngdbenginemodelsv3.IndexProperties{
							IndexDeletionBehaviour: ltngdbenginemodelsv3.IndexOnly,
						},
					}
					_, err = ts.e.DeleteItem(ts.ctx, databaseMetaInfo, item, deleteOpts)
					require.NoError(t, err)
				}

				{
					// search by key
					searchOpts := &ltngdbenginemodelsv3.IndexOpts{}
					loadedItem, err := ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					// search by key - parent key
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:       true,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
						IndexProperties: ltngdbenginemodelsv3.IndexProperties{
							IndexSearchPattern: ltngdbenginemodelsv3.AndComputational,
						},
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, nil, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)
				}

				{
					// list items - default search
					items, err := ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				ts.e.Close()
			})

			t.Run("upsert", func(t *testing.T) {
				ts := initTestSuite(t)
				dbInfo := createTestStore(t, ts.ctx, ts)
				userData := generateTestUser(t)
				bsValues := getValues(t, ts, userData)
				databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()

				// #################################################################################### \\

				item := &ltngdbenginemodelsv3.Item{
					Key:   bsValues.bsKey,
					Value: bsValues.bsValue,
				}
				createOpts := &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:       true,
					ParentKey:    item.Key,
					IndexingKeys: [][]byte{bsValues.bsKey, bsValues.secondaryIndexBs},
				}
				_, err := ts.e.CreateItem(ts.ctx, databaseMetaInfo, item, createOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &ltngdbenginemodelsv3.IndexOpts{}
					loadedItem, err := ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)
				}

				{
					// list items - default search
					items, err := ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				item = &ltngdbenginemodelsv3.Item{
					Key:   bsValues.bsKey,
					Value: bsValues.bsValue,
				}
				createOpts = &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:       true,
					ParentKey:    item.Key,
					IndexingKeys: [][]byte{bsValues.bsKey, bsValues.secondaryIndexBs, bsValues.extraUpsertIndex},
				}
				_, err = ts.e.UpsertItem(ts.ctx, databaseMetaInfo, item, createOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &ltngdbenginemodelsv3.IndexOpts{}
					loadedItem, err := ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.bsKey},
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.extraUpsertIndex},
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)
				}

				{
					// list items - default search
					items, err := ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				//t.Fatal()

				{
					deleteOpts := &ltngdbenginemodelsv3.IndexOpts{
						HasIdx: true,
						IndexProperties: ltngdbenginemodelsv3.IndexProperties{
							IndexDeletionBehaviour: ltngdbenginemodelsv3.Cascade,
						},
					}
					_, err = ts.e.DeleteItem(ts.ctx, databaseMetaInfo, item, deleteOpts)
					require.NoError(t, err)
				}

				{
					// search by key
					searchOpts := &ltngdbenginemodelsv3.IndexOpts{}
					loadedItem, err := ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by key - parent key
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by index
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)
				}

				{
					// list items - default search
					items, err := ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				ts.e.Close()
			})

			t.Run("detect last opened at difference after closing", func(t *testing.T) {
				ts := initTestSuite(t)
				dbInfo := createTestStore(t, ts.ctx, ts)
				userData := generateTestUser(t)
				bsValues := getValues(t, ts, userData)
				databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()

				// #################################################################################### \\

				item := &ltngdbenginemodelsv3.Item{
					Key:   bsValues.bsKey,
					Value: bsValues.bsValue,
				}
				createOpts := &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:       true,
					ParentKey:    item.Key,
					IndexingKeys: [][]byte{bsValues.bsKey, bsValues.secondaryIndexBs},
				}
				_, err := ts.e.CreateItem(ts.ctx, databaseMetaInfo, item, createOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &ltngdbenginemodelsv3.IndexOpts{}
					loadedItem, err := ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)
				}

				{
					// list items - default search
					items, err := ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				err = ts.e.Restart(ts.ctx)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &ltngdbenginemodelsv3.IndexOpts{}
					loadedItem, err := ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)
				}

				{
					// list items - default search
					items, err := ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				deleteOpts := &ltngdbenginemodelsv3.IndexOpts{
					HasIdx: true,
					IndexProperties: ltngdbenginemodelsv3.IndexProperties{
						IndexDeletionBehaviour: ltngdbenginemodelsv3.Cascade,
					},
				}
				_, err = ts.e.DeleteItem(ts.ctx, databaseMetaInfo, item, deleteOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &ltngdbenginemodelsv3.IndexOpts{}
					loadedItem, err := ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by key - parent key
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by index
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)
				}

				{
					// list items - default search
					items, err := ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				ts.e.Close()
			})

			t.Run("process and close", func(t *testing.T) {
				ts := initTestSuite(t)
				dbInfo := createTestStore(t, ts.ctx, ts)
				userData := generateTestUser(t)
				bsValues := getValues(t, ts, userData)
				databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()

				// #################################################################################### \\

				item := &ltngdbenginemodelsv3.Item{
					Key:   bsValues.bsKey,
					Value: bsValues.bsValue,
				}
				createOpts := &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:       true,
					ParentKey:    item.Key,
					IndexingKeys: [][]byte{bsValues.bsKey, bsValues.secondaryIndexBs},
				}
				_, err := ts.e.CreateItem(ts.ctx, databaseMetaInfo, item, createOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &ltngdbenginemodelsv3.IndexOpts{}
					loadedItem, err := ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					var u user
					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.NoError(t, err)
					require.NotNil(t, loadedItem)

					err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
					require.NoError(t, err)
					t.Log(u)
				}

				{
					// list items - default search
					items, err := ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				deleteOpts := &ltngdbenginemodelsv3.IndexOpts{
					HasIdx: true,
					IndexProperties: ltngdbenginemodelsv3.IndexProperties{
						IndexDeletionBehaviour: ltngdbenginemodelsv3.Cascade,
					},
				}
				_, err = ts.e.DeleteItem(ts.ctx, databaseMetaInfo, item, deleteOpts)
				require.NoError(t, err)

				{
					// search by key
					searchOpts := &ltngdbenginemodelsv3.IndexOpts{}
					loadedItem, err := ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by key - parent key
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)

					// search by index
					searchOpts = &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{bsValues.secondaryIndexBs},
					}
					loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, item, searchOpts)
					require.Error(t, err)
					require.Nil(t, loadedItem)
				}

				{
					// list items - default search
					items, err := ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.Default,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ts.e.ListItems(
						ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
						&ltngdbenginemodelsv3.IndexOpts{
							IndexProperties: ltngdbenginemodelsv3.IndexProperties{
								ListSearchPattern: ltngdbenginemodelsv3.All,
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				ts.e.Close()
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

				createOpts := &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:       true,
					ParentKey:    bvs.item.Key,
					IndexingKeys: [][]byte{bvs.bsKey, bvs.secondaryIndexBs},
				}
				_, err := ts.e.CreateItem(ts.ctx, databaseMetaInfo, bvs.item, createOpts)
				require.NoError(t, err)
			}

			// list ops
			{
				// list items - default search
				items, err := ts.e.ListItems(
					ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
					&ltngdbenginemodelsv3.IndexOpts{
						IndexProperties: ltngdbenginemodelsv3.IndexProperties{
							ListSearchPattern: ltngdbenginemodelsv3.Default,
						},
					},
				)
				require.NoError(t, err)
				require.Len(t, items.Items, userCount)

				for _, item := range items.Items {
					t.Log(string(item.Key), string(item.Value))
				}

				// list items - search for all
				items, err = ts.e.ListItems(
					ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
					&ltngdbenginemodelsv3.IndexOpts{
						IndexProperties: ltngdbenginemodelsv3.IndexProperties{
							ListSearchPattern: ltngdbenginemodelsv3.All,
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
				searchOpts := &ltngdbenginemodelsv3.IndexOpts{}
				loadedItem, err := ts.e.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.NoError(t, err)
				require.NotNil(t, loadedItem)

				var u user
				err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
				require.NoError(t, err)
				t.Log(u)

				// search by key - parent key
				searchOpts = &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:    true,
					ParentKey: bvs.item.Key,
				}
				loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.NoError(t, err)
				require.NotNil(t, loadedItem)

				err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
				require.NoError(t, err)
				t.Log(u)

				// search by index
				searchOpts = &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:       true,
					ParentKey:    bvs.item.Key,
					IndexingKeys: [][]byte{bvs.secondaryIndexBs},
				}
				loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.NoError(t, err)
				require.NotNil(t, loadedItem)

				err = ts.e.serializer.Deserialize(loadedItem.Value, &u)
				require.NoError(t, err)
				t.Log(u)
			}

			// delete and search
			for _, userData := range userList {
				bvs := getValues(t, ts, userData)

				deleteOpts := &ltngdbenginemodelsv3.IndexOpts{
					HasIdx: true,
					IndexProperties: ltngdbenginemodelsv3.IndexProperties{
						IndexDeletionBehaviour: ltngdbenginemodelsv3.Cascade,
					},
				}
				_, err := ts.e.DeleteItem(ts.ctx, databaseMetaInfo, bvs.item, deleteOpts)
				require.NoError(t, err)

				// search by key
				searchOpts := &ltngdbenginemodelsv3.IndexOpts{}
				loadedItem, err := ts.e.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				// t.Logf("key %s - value %s - err: %v", loadedItem.Key, loadedItem.Value, err)
				require.Error(t, err)
				require.Nil(t, loadedItem)

				// search by key - parent key
				searchOpts = &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:    true,
					ParentKey: bvs.item.Key,
				}
				loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.Error(t, err)
				require.Nil(t, loadedItem)

				// search by index
				searchOpts = &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:       true,
					ParentKey:    bvs.item.Key,
					IndexingKeys: [][]byte{bvs.secondaryIndexBs},
				}
				loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.Error(t, err)
				require.Nil(t, loadedItem)
			}

			// list ops
			{
				// list items - default search
				items, err := ts.e.ListItems(
					ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
					&ltngdbenginemodelsv3.IndexOpts{
						IndexProperties: ltngdbenginemodelsv3.IndexProperties{
							ListSearchPattern: ltngdbenginemodelsv3.Default,
						},
					},
				)
				require.NoError(t, err)
				require.Len(t, items.Items, 0)

				for _, item := range items.Items {
					t.Log(string(item.Key), string(item.Value))
				}

				// list items - search for all
				items, err = ts.e.ListItems(
					ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
					&ltngdbenginemodelsv3.IndexOpts{
						IndexProperties: ltngdbenginemodelsv3.IndexProperties{
							ListSearchPattern: ltngdbenginemodelsv3.All,
						},
					},
				)
				require.NoError(t, err)
				require.Len(t, items.Items, 0)

				for _, item := range items.Items {
					t.Log(string(item.Key), string(item.Value))
				}
			}

			ts.e.Close()
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
				searchOpts := &ltngdbenginemodelsv3.IndexOpts{}
				loadedItem, err := ts.e.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.Error(t, err)
				require.Nil(t, loadedItem)

				// search by key - parent key
				searchOpts = &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:    true,
					ParentKey: bvs.item.Key,
				}
				loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.Error(t, err)
				require.Nil(t, loadedItem)

				// search by index
				searchOpts = &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:       true,
					ParentKey:    bvs.item.Key,
					IndexingKeys: [][]byte{bvs.secondaryIndexBs},
				}
				loadedItem, err = ts.e.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				require.Error(t, err)
				require.Nil(t, loadedItem)
			}

			{
				// list items - default search
				items, err := ts.e.ListItems(
					ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
					&ltngdbenginemodelsv3.IndexOpts{
						IndexProperties: ltngdbenginemodelsv3.IndexProperties{
							ListSearchPattern: ltngdbenginemodelsv3.Default,
						},
					},
				)
				require.NoError(t, err)
				require.Len(t, items.Items, 0)

				for _, item := range items.Items {
					t.Log(string(item.Key), string(item.Value))
				}

				// list items - search for all
				items, err = ts.e.ListItems(
					ts.ctx, databaseMetaInfo, ltngdata.PageDefault(1),
					&ltngdbenginemodelsv3.IndexOpts{
						IndexProperties: ltngdbenginemodelsv3.IndexProperties{
							ListSearchPattern: ltngdbenginemodelsv3.All,
						},
					},
				)
				require.NoError(t, err)
				require.Len(t, items.Items, 0)

				for _, item := range items.Items {
					t.Log(string(item.Key), string(item.Value))
				}
			}

			ts.e.Close()
		})
	})

	t.Run("test on and off", func(t *testing.T) {
		ts := initTestSuite(t)
		dbInfo := createTestStore(t, ts.ctx, ts)
		_ = dbInfo
		_ = ts

		ts.e.Close()
	})
}

func TestReadFromFQ(t *testing.T) {
	fq, err := mmap.NewFileQueue(fileiomodels.GetFileQueueFilePath(fileiomodels.FileQueueMmapVersion,
		fileiomodels.GenericFileQueueFilePath, fileiomodels.GenericFileQueueFileName))
	require.NoError(t, err)

	var counter int
	for {
		_, err = fq.Read()
		if err != nil {
			t.Log(err)
			break
		}

		_, err = fq.Pop()
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
	err = osx.DelHard(ctx, ltngdbenginemodelsv3.DBBasePath)
	require.NoError(t, err)

	return ctx
}

func prepareTestWithCancel(t *testing.T) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	err := osx.DelHard(ctx, fileiomodels.FileQueueBasePath)
	err = osx.DelHard(ctx, ltngdbenginemodelsv3.DBBasePath)
	require.NoError(t, err)

	return ctx, cancel
}

func assertLOFromHistory(t *testing.T, history []*ltngdbenginemodelsv3.StoreInfo) {
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
		ctx    context.Context
		cancel context.CancelFunc

		e *LTNGEngine

		cs *createSaga
		us *upsertSaga
		ds *deleteSaga
	}
)

type bytesValues struct {
	bsKey, bsValue, secondaryIndexBs, extraUpsertIndex []byte
	item                                               *ltngdbenginemodelsv3.Item
}

func initTestSuite(t *testing.T) *testSuite {
	ctx, cancel := context.WithCancel(prepareTest(t))
	e, err := New(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		e.Close()
	})

	return &testSuite{
		ctx:    ctx,
		cancel: cancel,

		e: e,
	}
}

func setUserTime(userData *user) {
	tn := time.Now().UTC().Unix()
	userData.CreatedAt = tn
	userData.UpdatedAt = tn
}

func getValues(t *testing.T, ts *testSuite, userData *user) *bytesValues {
	setUserTime(userData)

	bsKey, err := ts.e.serializer.Serialize(userData.Email)
	require.NoError(t, err)
	require.NotNil(t, bsKey)
	//t.Log(string(bsKey))

	bsValue, err := ts.e.serializer.Serialize(userData)
	require.NoError(t, err)
	require.NotNil(t, bsValue)
	//t.Log(string(bsValue))

	secondaryIndexBs, err := ts.e.serializer.Serialize(userData.Username)
	require.NoError(t, err)
	require.NotNil(t, secondaryIndexBs)
	//t.Log(string(secondaryIndexBs))

	extraUpsertIndexBs, err := ts.e.serializer.Serialize(userData.UUID)
	require.NoError(t, err)
	require.NotNil(t, extraUpsertIndexBs)
	//t.Log(string(extraUpsertIndexBs))

	return &bytesValues{
		bsKey:            bsKey,
		bsValue:          bsValue,
		secondaryIndexBs: secondaryIndexBs,
		extraUpsertIndex: extraUpsertIndexBs,
		item: &ltngdbenginemodelsv3.Item{
			Key:   bsKey,
			Value: bsValue,
		},
	}
}

func createTestStore(t *testing.T, ctx context.Context, ts *testSuite) *ltngdbenginemodelsv3.StoreInfo {
	dbInfo := &ltngdbenginemodelsv3.StoreInfo{
		Name: "test-store",
		Path: "test-path",
	}
	info, err := ts.e.CreateStore(ctx, dbInfo)
	require.NoError(t, err)
	require.NotNil(t, info)

	info, err = ts.e.LoadStore(ctx, dbInfo)
	require.NoError(t, err)

	infos, err := ts.e.ListStores(ctx, &ltngdata.Pagination{
		PageID:   1,
		PageSize: 5,
	})
	require.NoError(t, err)
	require.Len(t, infos, 1)
	//t.Log(infos)

	//for _, info = range infos {
	//	t.Log(info)
	//}

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
	//t.Log(userData.Email)

	return userData
}

func generateTestUsers(t *testing.T, n int) []*user {
	users := make([]*user, n)
	for i := 0; i < n; i++ {
		users[i] = generateTestUser(t)
	}

	return users
}

func generateItemInfoData(
	t *testing.T,
	ts *testSuite,
	hasIndex bool,
) *ltngdbenginemodelsv3.ItemInfoData {
	storeInfo := createTestStore(t, ts.ctx, ts)

	testUser := generateTestUser(t)
	byteValues := getValues(t, ts, testUser)

	traceID, err := uuid.NewV7()
	require.NoError(t, err)
	respSignal := make(chan error)
	itemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
		Ctx:          ts.ctx,
		RespSignal:   respSignal,
		TraceID:      traceID.String(),
		OpNatureType: ltngdbenginemodelsv3.OpNatureTypeItem,
		OpType:       ltngdbenginemodelsv3.OpTypeCreate,
		DBMetaInfo:   storeInfo.ManagerStoreMetaInfo(),
		Item: &ltngdbenginemodelsv3.Item{
			Key:   byteValues.bsKey,
			Value: byteValues.bsValue,
		},
		Opts: nil,
	}
	if hasIndex {
		itemInfoData.Opts = &ltngdbenginemodelsv3.IndexOpts{
			HasIdx:    true,
			ParentKey: byteValues.bsKey,
			IndexingKeys: [][]byte{
				byteValues.bsKey,
				byteValues.secondaryIndexBs,
				byteValues.extraUpsertIndex,
			},
		}
	} else {
		itemInfoData.Opts = &ltngdbenginemodelsv3.IndexOpts{
			HasIdx: false,
		}
	}

	return itemInfoData
}

func generateItemInfoDataList(
	t *testing.T,
	ts *testSuite,
	itemCount int,
	hasIndex bool,
) []*ltngdbenginemodelsv3.ItemInfoData {
	storeInfo := createTestStore(t, ts.ctx, ts)

	testUsers := generateTestUsers(t, itemCount)
	itemInfoDataList := make([]*ltngdbenginemodelsv3.ItemInfoData, itemCount)
	for idx, testUser := range testUsers {
		byteValues := getValues(t, ts, testUser)

		traceID, err := uuid.NewV7()
		require.NoError(t, err)
		respSignal := make(chan error)
		itemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
			Ctx:          ts.ctx,
			RespSignal:   respSignal,
			TraceID:      traceID.String(),
			OpNatureType: ltngdbenginemodelsv3.OpNatureTypeItem,
			OpType:       ltngdbenginemodelsv3.OpTypeCreate,
			DBMetaInfo:   storeInfo.ManagerStoreMetaInfo(),
			Item: &ltngdbenginemodelsv3.Item{
				Key:   byteValues.bsKey,
				Value: byteValues.bsValue,
			},
			Opts: nil,
		}
		if hasIndex {
			itemInfoData.Opts = &ltngdbenginemodelsv3.IndexOpts{
				HasIdx:    true,
				ParentKey: byteValues.bsKey,
				IndexingKeys: [][]byte{
					byteValues.bsKey,
					byteValues.secondaryIndexBs,
					byteValues.extraUpsertIndex,
				},
			}
		} else {
			itemInfoData.Opts = &ltngdbenginemodelsv3.IndexOpts{
				HasIdx: false,
			}
		}

		itemInfoDataList[idx] = itemInfoData
	}

	return itemInfoDataList
}

func updateItemInfoData(
	t *testing.T,
	ts *testSuite,
	itemInfoData *ltngdbenginemodelsv3.ItemInfoData,
	hasIndex bool,
) *ltngdbenginemodelsv3.ItemInfoData {
	var userData user
	err := ts.e.serializer.Deserialize(itemInfoData.Item.Value, &userData)
	require.NoError(t, err)

	newUUID, err := uuid.NewV7()
	require.NoError(t, err)
	userData.UUID = newUUID.String()

	byteValues := getValues(t, ts, &userData)

	traceID, err := uuid.NewV7()
	require.NoError(t, err)

	updatedItemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
		Ctx:          ts.ctx,
		RespSignal:   itemInfoData.RespSignal,
		TraceID:      traceID.String(),
		OpNatureType: itemInfoData.OpNatureType,
		OpType:       itemInfoData.OpType,
		DBMetaInfo:   itemInfoData.DBMetaInfo,
		Item: &ltngdbenginemodelsv3.Item{
			Key:   byteValues.bsKey,
			Value: byteValues.bsValue,
		},
		Opts: nil,
	}
	if hasIndex {
		updatedItemInfoData.Opts = &ltngdbenginemodelsv3.IndexOpts{
			HasIdx:    true,
			ParentKey: byteValues.bsKey,
			IndexingKeys: [][]byte{
				byteValues.bsKey,
				byteValues.secondaryIndexBs,
				byteValues.extraUpsertIndex,
			},
		}
	} else {
		updatedItemInfoData.Opts = &ltngdbenginemodelsv3.IndexOpts{
			HasIdx: false,
		}
	}

	return updatedItemInfoData
}
