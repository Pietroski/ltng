package v1

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	go_random "gitlab.com/pietroski-software-company/tools/random/go-random/pkg/tools/random"

	ltng_engine_models "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
)

func TestLTNGEngineFlow(t *testing.T) {
	t.Run("store crud tests", func(t *testing.T) {
		t.Run("for single store", func(t *testing.T) {
			t.Run("standard", func(t *testing.T) {
				ctx := prepareTest(t)

				ltngEngine, err := New(ctx)
				require.NoError(t, err)

				info, err := ltngEngine.CreateStore(ctx, &StoreInfo{
					Name: "test-store",
					Path: "test-path",
				})
				assert.NoError(t, err)
				assert.NotNil(t, info)
				t.Log(info)

				info, err = ltngEngine.LoadStore(ctx, &StoreInfo{
					Name: "test-store",
					Path: "test-path",
				})
				assert.NoError(t, err)
				t.Log(info)

				err = ltngEngine.DeleteStore(ctx, &StoreInfo{
					Name: "test-store",
					Path: "test-path",
				})
				assert.NoError(t, err)

				info, err = ltngEngine.LoadStore(ctx, &StoreInfo{
					Name: "test-store",
					Path: "test-path",
				})
				assert.Error(t, err)
				assert.Nil(t, info)
				t.Log(info)
			})

			t.Run("detect last opened at difference after closing", func(t *testing.T) {
				t.Run("with close only", func(t *testing.T) {
					ctx := prepareTest(t)

					ltngEngine, err := New(ctx)
					require.NoError(t, err)
					infoHistory := make([]*StoreInfo, 0)

					info, err := ltngEngine.CreateStore(ctx, &StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					assert.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					info, err = ltngEngine.LoadStore(ctx, &StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					assert.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					ltngEngine.Close()
					time.Sleep(1 * time.Second)

					info, err = ltngEngine.LoadStore(ctx, &StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					assert.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					infos, err := ltngEngine.ListStores(ctx, &ltng_engine_models.Pagination{
						PageID:   1,
						PageSize: 5,
					})
					assert.NoError(t, err)
					assert.Len(t, infos, 1)
					t.Log(infos)

					for _, info = range infos {
						t.Log(info)
					}

					err = ltngEngine.DeleteStore(ctx, &StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					assert.NoError(t, err)

					info, err = ltngEngine.LoadStore(ctx, &StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					assert.Error(t, err)
					assert.Nil(t, info)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					assertLOFromHistory(t, infoHistory)
				})

				t.Run("with restart", func(t *testing.T) {
					ctx := prepareTest(t)

					ltngEngine, err := New(ctx)
					require.NoError(t, err)
					infoHistory := make([]*StoreInfo, 0)

					info, err := ltngEngine.CreateStore(ctx, &StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					assert.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					info, err = ltngEngine.LoadStore(ctx, &StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					assert.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					err = ltngEngine.Restart(ctx)
					assert.NoError(t, err)
					time.Sleep(1 * time.Second)

					info, err = ltngEngine.LoadStore(ctx, &StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					assert.NoError(t, err)
					infoHistory = append(infoHistory, info)
					t.Log(info)

					infos, err := ltngEngine.ListStores(ctx, &ltng_engine_models.Pagination{
						PageID:   1,
						PageSize: 5,
					})
					assert.NoError(t, err)
					assert.Len(t, infos, 1)
					t.Log(infos)

					for _, info = range infos {
						t.Log(info)
					}

					err = ltngEngine.DeleteStore(ctx, &StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					assert.NoError(t, err)

					info, err = ltngEngine.LoadStore(ctx, &StoreInfo{
						Name: "test-store",
						Path: "test-path",
					})
					assert.Error(t, err)
					assert.Nil(t, info)
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

			info, err := ltngEngine.CreateStore(ctx, &StoreInfo{
				Name: "test-store",
				Path: "test-path",
			})
			assert.NoError(t, err)
			assert.NotNil(t, info)

			info, err = ltngEngine.CreateStore(ctx, &StoreInfo{
				Name: "test-another-store",
				Path: "test-another-path",
			})
			assert.NoError(t, err)

			infos, err := ltngEngine.ListStores(ctx, &ltng_engine_models.Pagination{
				PageID:   1,
				PageSize: 5,
			})
			assert.NoError(t, err)
			assert.Len(t, infos, 2)
			t.Log(infos)

			for _, info = range infos {
				t.Log(info)
			}

			err = ltngEngine.DeleteStore(ctx, &StoreInfo{
				Name: "test-store",
				Path: "test-path",
			})
			assert.NoError(t, err)

			err = ltngEngine.DeleteStore(ctx, &StoreInfo{
				Name: "test-another-store",
				Path: "test-another-path",
			})
			assert.NoError(t, err)

			infos, err = ltngEngine.ListStores(ctx, &ltng_engine_models.Pagination{
				PageID:   1,
				PageSize: 5,
			})
			assert.NoError(t, err)
			assert.Len(t, infos, 0)
			t.Log(infos, err)

			info, err = ltngEngine.LoadStore(ctx, dbManagerStoreInfo.RelationalInfo())
			assert.NoError(t, err)
			assert.NotNil(t, info)
			t.Log(info)
		})

		t.Run("for inexistent store", func(t *testing.T) {
			ctx := prepareTest(t)

			ltngEngine, err := New(ctx)
			require.NoError(t, err)

			err = ltngEngine.DeleteStore(ctx, &StoreInfo{
				Name: "delete-inexistent-store",
				Path: "delete-inexistent-path",
			})
			assert.Error(t, err)
		})
	})

	t.Run("item crud tests", func(t *testing.T) {
		t.Run("for single item", func(t *testing.T) {
			t.Run("standard", func(t *testing.T) {
				ctx := prepareTest(t)
				ltngEngine, err := New(ctx)
				require.NoError(t, err)

				dbInfo := &StoreInfo{
					Name: "test-store",
					Path: "test-path",
				}
				info, err := ltngEngine.CreateStore(ctx, dbInfo)
				assert.NoError(t, err)
				assert.NotNil(t, info)

				info, err = ltngEngine.LoadStore(ctx, dbInfo)
				assert.NoError(t, err)

				infos, err := ltngEngine.ListStores(ctx, &ltng_engine_models.Pagination{
					PageID:   1,
					PageSize: 5,
				})
				assert.NoError(t, err)
				assert.Len(t, infos, 1)
				t.Log(infos)

				for _, info = range infos {
					t.Log(info)
				}

				// #################################################################################### \\

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

				bsKey, err := ltngEngine.serializer.Serialize(userData.Email)
				require.NoError(t, err)
				require.NotNil(t, bsKey)
				t.Log(string(bsKey))

				bsValue, err := ltngEngine.serializer.Serialize(userData)
				require.NoError(t, err)
				require.NotNil(t, bsValue)
				t.Log(string(bsValue))

				secondaryIndexBs, err := ltngEngine.serializer.Serialize(userData.Username)
				require.NoError(t, err)
				require.NotNil(t, secondaryIndexBs)
				t.Log(string(secondaryIndexBs))

				databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()
				item := &Item{
					Key:   bsKey,
					Value: bsValue,
				}
				createOpts := &IndexOpts{
					HasIdx:       true,
					ParentKey:    item.Key,
					IndexingKeys: [][]byte{bsKey, secondaryIndexBs},
				}
				_, err = ltngEngine.CreateItem(ctx, databaseMetaInfo, item, createOpts)
				assert.NoError(t, err)

				{
					// search by key
					searchOpts := &IndexOpts{}
					loadedItem, err := ltngEngine.LoadItem(ctx, databaseMetaInfo, item, searchOpts)
					assert.NoError(t, err)
					assert.NotNil(t, loadedItem)

					var u user
					err = ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					assert.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ltngEngine.LoadItem(ctx, databaseMetaInfo, item, searchOpts)
					assert.NoError(t, err)
					assert.NotNil(t, loadedItem)

					err = ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					assert.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{secondaryIndexBs},
					}
					loadedItem, err = ltngEngine.LoadItem(ctx, databaseMetaInfo, item, searchOpts)
					assert.NoError(t, err)
					assert.NotNil(t, loadedItem)

					err = ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					assert.NoError(t, err)
					t.Log(u)
				}

				{
					// list items - default search
					items, err := ltngEngine.ListItems(
						ctx, databaseMetaInfo, ltng_engine_models.PageDefault(1),
						&IndexOpts{
							IndexProperties: IndexProperties{
								ListSearchPattern: Default,
							},
						},
					)
					assert.NoError(t, err)
					assert.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ltngEngine.ListItems(
						ctx, databaseMetaInfo, ltng_engine_models.PageDefault(1),
						&IndexOpts{
							IndexProperties: IndexProperties{
								ListSearchPattern: All,
							},
						},
					)
					assert.NoError(t, err)
					assert.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				deleteOpts := &IndexOpts{
					IndexProperties: IndexProperties{
						IndexDeletionBehaviour: Cascade,
					},
				}
				_, err = ltngEngine.DeleteItem(ctx, databaseMetaInfo, item, deleteOpts)
				assert.NoError(t, err)

				{
					// search by key
					searchOpts := &IndexOpts{}
					loadedItem, err := ltngEngine.LoadItem(ctx, databaseMetaInfo, item, searchOpts)
					assert.Error(t, err)
					assert.Nil(t, loadedItem)

					// search by key - parent key
					searchOpts = &IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ltngEngine.LoadItem(ctx, databaseMetaInfo, item, searchOpts)
					assert.Error(t, err)
					assert.Nil(t, loadedItem)

					// search by index
					searchOpts = &IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{secondaryIndexBs},
					}
					loadedItem, err = ltngEngine.LoadItem(ctx, databaseMetaInfo, item, searchOpts)
					assert.Error(t, err)
					assert.Nil(t, loadedItem)
				}

				{
					// list items - default search
					items, err := ltngEngine.ListItems(
						ctx, databaseMetaInfo, ltng_engine_models.PageDefault(1),
						&IndexOpts{
							IndexProperties: IndexProperties{
								ListSearchPattern: Default,
							},
						},
					)
					assert.NoError(t, err)
					assert.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ltngEngine.ListItems(
						ctx, databaseMetaInfo, ltng_engine_models.PageDefault(1),
						&IndexOpts{
							IndexProperties: IndexProperties{
								ListSearchPattern: All,
							},
						},
					)
					assert.NoError(t, err)
					assert.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}
			})

			t.Run("detect last opened at difference after closing", func(t *testing.T) {
				ctx := prepareTest(t)
				ltngEngine, err := New(ctx)
				require.NoError(t, err)

				dbInfo := &StoreInfo{
					Name: "test-store",
					Path: "test-path",
				}
				info, err := ltngEngine.CreateStore(ctx, dbInfo)
				assert.NoError(t, err)
				assert.NotNil(t, info)

				info, err = ltngEngine.LoadStore(ctx, dbInfo)
				assert.NoError(t, err)

				infos, err := ltngEngine.ListStores(ctx, &ltng_engine_models.Pagination{
					PageID:   1,
					PageSize: 5,
				})
				assert.NoError(t, err)
				assert.Len(t, infos, 1)
				t.Log(infos)

				for _, info = range infos {
					t.Log(info)
				}

				// #################################################################################### \\

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

				bsKey, err := ltngEngine.serializer.Serialize(userData.Email)
				require.NoError(t, err)
				require.NotNil(t, bsKey)
				t.Log(string(bsKey))

				bsValue, err := ltngEngine.serializer.Serialize(userData)
				require.NoError(t, err)
				require.NotNil(t, bsValue)
				t.Log(string(bsValue))

				secondaryIndexBs, err := ltngEngine.serializer.Serialize(userData.Username)
				require.NoError(t, err)
				require.NotNil(t, secondaryIndexBs)
				t.Log(string(secondaryIndexBs))

				databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()
				item := &Item{
					Key:   bsKey,
					Value: bsValue,
				}
				createOpts := &IndexOpts{
					HasIdx:       true,
					ParentKey:    item.Key,
					IndexingKeys: [][]byte{bsKey, secondaryIndexBs},
				}
				_, err = ltngEngine.CreateItem(ctx, databaseMetaInfo, item, createOpts)
				assert.NoError(t, err)

				{
					// search by key
					searchOpts := &IndexOpts{}
					loadedItem, err := ltngEngine.LoadItem(ctx, databaseMetaInfo, item, searchOpts)
					assert.NoError(t, err)
					assert.NotNil(t, loadedItem)

					var u user
					err = ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					assert.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ltngEngine.LoadItem(ctx, databaseMetaInfo, item, searchOpts)
					assert.NoError(t, err)
					assert.NotNil(t, loadedItem)

					err = ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					assert.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{secondaryIndexBs},
					}
					loadedItem, err = ltngEngine.LoadItem(ctx, databaseMetaInfo, item, searchOpts)
					assert.NoError(t, err)
					assert.NotNil(t, loadedItem)

					err = ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					assert.NoError(t, err)
					t.Log(u)
				}

				{
					// list items - default search
					items, err := ltngEngine.ListItems(
						ctx, databaseMetaInfo, ltng_engine_models.PageDefault(1),
						&IndexOpts{
							IndexProperties: IndexProperties{
								ListSearchPattern: Default,
							},
						},
					)
					assert.NoError(t, err)
					assert.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ltngEngine.ListItems(
						ctx, databaseMetaInfo, ltng_engine_models.PageDefault(1),
						&IndexOpts{
							IndexProperties: IndexProperties{
								ListSearchPattern: All,
							},
						},
					)
					assert.NoError(t, err)
					assert.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				ltngEngine.close()

				{
					// search by key
					searchOpts := &IndexOpts{}
					loadedItem, err := ltngEngine.LoadItem(ctx, databaseMetaInfo, item, searchOpts)
					assert.NoError(t, err)
					assert.NotNil(t, loadedItem)

					var u user
					err = ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					assert.NoError(t, err)
					t.Log(u)

					// search by key - parent key
					searchOpts = &IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ltngEngine.LoadItem(ctx, databaseMetaInfo, item, searchOpts)
					assert.NoError(t, err)
					assert.NotNil(t, loadedItem)

					err = ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					assert.NoError(t, err)
					t.Log(u)

					// search by index
					searchOpts = &IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{secondaryIndexBs},
					}
					loadedItem, err = ltngEngine.LoadItem(ctx, databaseMetaInfo, item, searchOpts)
					assert.NoError(t, err)
					assert.NotNil(t, loadedItem)

					err = ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
					assert.NoError(t, err)
					t.Log(u)
				}

				{
					// list items - default search
					items, err := ltngEngine.ListItems(
						ctx, databaseMetaInfo, ltng_engine_models.PageDefault(1),
						&IndexOpts{
							IndexProperties: IndexProperties{
								ListSearchPattern: Default,
							},
						},
					)
					assert.NoError(t, err)
					assert.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ltngEngine.ListItems(
						ctx, databaseMetaInfo, ltng_engine_models.PageDefault(1),
						&IndexOpts{
							IndexProperties: IndexProperties{
								ListSearchPattern: All,
							},
						},
					)
					assert.NoError(t, err)
					assert.Len(t, items.Items, 1)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}

				deleteOpts := &IndexOpts{
					IndexProperties: IndexProperties{
						IndexDeletionBehaviour: Cascade,
					},
				}
				_, err = ltngEngine.DeleteItem(ctx, databaseMetaInfo, item, deleteOpts)
				assert.NoError(t, err)

				{
					// search by key
					searchOpts := &IndexOpts{}
					loadedItem, err := ltngEngine.LoadItem(ctx, databaseMetaInfo, item, searchOpts)
					assert.Error(t, err)
					assert.Nil(t, loadedItem)

					// search by key - parent key
					searchOpts = &IndexOpts{
						HasIdx:    true,
						ParentKey: item.Key,
					}
					loadedItem, err = ltngEngine.LoadItem(ctx, databaseMetaInfo, item, searchOpts)
					assert.Error(t, err)
					assert.Nil(t, loadedItem)

					// search by index
					searchOpts = &IndexOpts{
						HasIdx:       true,
						ParentKey:    item.Key,
						IndexingKeys: [][]byte{secondaryIndexBs},
					}
					loadedItem, err = ltngEngine.LoadItem(ctx, databaseMetaInfo, item, searchOpts)
					assert.Error(t, err)
					assert.Nil(t, loadedItem)
				}

				{
					// list items - default search
					items, err := ltngEngine.ListItems(
						ctx, databaseMetaInfo, ltng_engine_models.PageDefault(1),
						&IndexOpts{
							IndexProperties: IndexProperties{
								ListSearchPattern: Default,
							},
						},
					)
					assert.NoError(t, err)
					assert.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}

					// list items - search for all
					items, err = ltngEngine.ListItems(
						ctx, databaseMetaInfo, ltng_engine_models.PageDefault(1),
						&IndexOpts{
							IndexProperties: IndexProperties{
								ListSearchPattern: All,
							},
						},
					)
					assert.NoError(t, err)
					assert.Len(t, items.Items, 0)

					for _, item = range items.Items {
						t.Log(string(item.Key), string(item.Value))
					}
				}
			})
		})

		t.Run("for multiple items", func(t *testing.T) {
			ts := initTestSuite(t)

			dbInfo := &StoreInfo{
				Name: "test-store",
				Path: "test-path",
			}
			info, err := ts.ltngEngine.CreateStore(ts.ctx, dbInfo)
			assert.NoError(t, err)
			assert.NotNil(t, info)

			info, err = ts.ltngEngine.LoadStore(ts.ctx, dbInfo)
			assert.NoError(t, err)

			infos, err := ts.ltngEngine.ListStores(ts.ctx, &ltng_engine_models.Pagination{
				PageID:   1,
				PageSize: 5,
			})
			assert.NoError(t, err)
			assert.Len(t, infos, 1)
			t.Log(infos)

			for _, info = range infos {
				t.Log(info)
			}

			testCases := []struct {
				userData *user
			}{
				{
					userData: &user{
						Username: go_random.RandomStringWithPrefixWithSep(12, "username", "-"),
						Password: go_random.RandomStringWithPrefixWithSep(12, "password", "-"),
						Email:    go_random.RandomEmail(),
						Name:     go_random.RandomStringWithPrefixWithSep(12, "name", "-"),
						Surname:  go_random.RandomStringWithPrefixWithSep(12, "surname", "-"),
						Age:      uint8(go_random.RandomInt(0, math.MaxUint8)),
					},
				},
				{
					userData: &user{
						Username: go_random.RandomStringWithPrefixWithSep(12, "another-username", "-"),
						Password: go_random.RandomStringWithPrefixWithSep(12, "another-password", "-"),
						Email:    go_random.RandomEmail(),
						Name:     go_random.RandomStringWithPrefixWithSep(12, "another-name", "-"),
						Surname:  go_random.RandomStringWithPrefixWithSep(12, "another-surname", "-"),
						Age:      uint8(go_random.RandomInt(0, math.MaxUint8)),
					},
				},
			}

			databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()

			for _, tc := range testCases {
				userData := tc.userData
				t.Log(userData.Email)

				bvs := getValues(t, ts, userData)

				createOpts := &IndexOpts{
					HasIdx:       true,
					ParentKey:    bvs.item.Key,
					IndexingKeys: [][]byte{bvs.bsKey, bvs.secondaryIndexBs},
				}
				_, err = ts.ltngEngine.CreateItem(ts.ctx, databaseMetaInfo, bvs.item, createOpts)
				assert.NoError(t, err)
			}

			// list ops
			{
				// list items - default search
				items, err := ts.ltngEngine.ListItems(
					ts.ctx, databaseMetaInfo, ltng_engine_models.PageDefault(1),
					&IndexOpts{
						IndexProperties: IndexProperties{
							ListSearchPattern: Default,
						},
					},
				)
				assert.NoError(t, err)
				assert.Len(t, items.Items, 2)

				for _, item := range items.Items {
					t.Log(string(item.Key), string(item.Value))
				}

				// list items - search for all
				items, err = ts.ltngEngine.ListItems(
					ts.ctx, databaseMetaInfo, ltng_engine_models.PageDefault(1),
					&IndexOpts{
						IndexProperties: IndexProperties{
							ListSearchPattern: All,
						},
					},
				)
				assert.NoError(t, err)
				assert.Len(t, items.Items, 2)

				for _, item := range items.Items {
					t.Log(string(item.Key), string(item.Value))
				}
			}

			// search one by one
			for _, tc := range testCases {
				bvs := getValues(t, ts, tc.userData)

				// search by key
				searchOpts := &IndexOpts{}
				loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				assert.NoError(t, err)
				assert.NotNil(t, loadedItem)

				var u user
				err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
				assert.NoError(t, err)
				t.Log(u)

				// search by key - parent key
				searchOpts = &IndexOpts{
					HasIdx:    true,
					ParentKey: bvs.item.Key,
				}
				loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				assert.NoError(t, err)
				assert.NotNil(t, loadedItem)

				err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
				assert.NoError(t, err)
				t.Log(u)

				// search by index
				searchOpts = &IndexOpts{
					HasIdx:       true,
					ParentKey:    bvs.item.Key,
					IndexingKeys: [][]byte{bvs.secondaryIndexBs},
				}
				loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				assert.NoError(t, err)
				assert.NotNil(t, loadedItem)

				err = ts.ltngEngine.serializer.Deserialize(loadedItem.Value, &u)
				assert.NoError(t, err)
				t.Log(u)
			}

			// delete and search
			for _, tc := range testCases {
				bvs := getValues(t, ts, tc.userData)

				deleteOpts := &IndexOpts{
					IndexProperties: IndexProperties{
						IndexDeletionBehaviour: Cascade,
					},
				}
				_, err = ts.ltngEngine.DeleteItem(ts.ctx, databaseMetaInfo, bvs.item, deleteOpts)
				assert.NoError(t, err)

				// search by key
				searchOpts := &IndexOpts{}
				loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				assert.Error(t, err)
				assert.Nil(t, loadedItem)

				// search by key - parent key
				searchOpts = &IndexOpts{
					HasIdx:    true,
					ParentKey: bvs.item.Key,
				}
				loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				assert.Error(t, err)
				assert.Nil(t, loadedItem)

				// search by index
				searchOpts = &IndexOpts{
					HasIdx:       true,
					ParentKey:    bvs.item.Key,
					IndexingKeys: [][]byte{bvs.secondaryIndexBs},
				}
				loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				assert.Error(t, err)
				assert.Nil(t, loadedItem)
			}

			// list ops
			{
				// list items - default search
				items, err := ts.ltngEngine.ListItems(
					ts.ctx, databaseMetaInfo, ltng_engine_models.PageDefault(1),
					&IndexOpts{
						IndexProperties: IndexProperties{
							ListSearchPattern: Default,
						},
					},
				)
				assert.NoError(t, err)
				assert.Len(t, items.Items, 0)

				for _, item := range items.Items {
					t.Log(string(item.Key), string(item.Value))
				}

				// list items - search for all
				items, err = ts.ltngEngine.ListItems(
					ts.ctx, databaseMetaInfo, ltng_engine_models.PageDefault(1),
					&IndexOpts{
						IndexProperties: IndexProperties{
							ListSearchPattern: All,
						},
					},
				)
				assert.NoError(t, err)
				assert.Len(t, items.Items, 0)

				for _, item := range items.Items {
					t.Log(string(item.Key), string(item.Value))
				}
			}
		})

		t.Run("for inexistent item", func(t *testing.T) {
			ts := initTestSuite(t)
			dbInfo := createTestStore(t, ts.ctx, ts)
			databaseMetaInfo := dbInfo.ManagerStoreMetaInfo()

			// #################################################################################### \\

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

			bvs := getValues(t, ts, userData)

			{
				// search by key
				searchOpts := &IndexOpts{}
				loadedItem, err := ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				assert.Error(t, err)
				assert.Nil(t, loadedItem)

				// search by key - parent key
				searchOpts = &IndexOpts{
					HasIdx:    true,
					ParentKey: bvs.item.Key,
				}
				loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				assert.Error(t, err)
				assert.Nil(t, loadedItem)

				// search by index
				searchOpts = &IndexOpts{
					HasIdx:       true,
					ParentKey:    bvs.item.Key,
					IndexingKeys: [][]byte{bvs.secondaryIndexBs},
				}
				loadedItem, err = ts.ltngEngine.LoadItem(ts.ctx, databaseMetaInfo, bvs.item, searchOpts)
				assert.Error(t, err)
				assert.Nil(t, loadedItem)
			}

			{
				// list items - default search
				items, err := ts.ltngEngine.ListItems(
					ts.ctx, databaseMetaInfo, ltng_engine_models.PageDefault(1),
					&IndexOpts{
						IndexProperties: IndexProperties{
							ListSearchPattern: Default,
						},
					},
				)
				assert.NoError(t, err)
				assert.Len(t, items.Items, 0)

				for _, item := range items.Items {
					t.Log(string(item.Key), string(item.Value))
				}

				// list items - search for all
				items, err = ts.ltngEngine.ListItems(
					ts.ctx, databaseMetaInfo, ltng_engine_models.PageDefault(1),
					&IndexOpts{
						IndexProperties: IndexProperties{
							ListSearchPattern: All,
						},
					},
				)
				assert.NoError(t, err)
				assert.Len(t, items.Items, 0)

				for _, item := range items.Items {
					t.Log(string(item.Key), string(item.Value))
				}
			}
		})
	})
}

func prepareTest(t *testing.T) context.Context {
	ctx := context.Background()
	_, err := delHardExec(ctx, dbBasePath)
	require.NoError(t, err)

	return ctx
}

func assertLOFromHistory(t *testing.T, history []*StoreInfo) {
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
	assert.True(t, differentLastOpenedCheck)
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
	item                             *Item
}

func initTestSuite(t *testing.T) *testSuite {
	ctx := prepareTest(t)
	ltngEngine, err := New(ctx)
	require.NoError(t, err)

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
		item: &Item{
			Key:   bsKey,
			Value: bsValue,
		},
	}
}

func createTestStore(t *testing.T, ctx context.Context, ts *testSuite) *StoreInfo {
	dbInfo := &StoreInfo{
		Name: "test-store",
		Path: "test-path",
	}
	info, err := ts.ltngEngine.CreateStore(ctx, dbInfo)
	assert.NoError(t, err)
	assert.NotNil(t, info)

	info, err = ts.ltngEngine.LoadStore(ctx, dbInfo)
	assert.NoError(t, err)

	infos, err := ts.ltngEngine.ListStores(ctx, &ltng_engine_models.Pagination{
		PageID:   1,
		PageSize: 5,
	})
	assert.NoError(t, err)
	assert.Len(t, infos, 1)
	t.Log(infos)

	for _, info = range infos {
		t.Log(info)
	}

	return info
}
