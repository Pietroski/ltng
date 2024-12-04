package v1

import (
	"context"
	ltng_engine_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/ltng-engine/v1"
	go_random "gitlab.com/pietroski-software-company/tools/random/go-random/pkg/tools/random"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

			bsValue, err := ltngEngine.serializer.Serialize(userData)
			require.NoError(t, err)
			require.NotNil(t, bsValue)

			secondaryIndexBs, err := ltngEngine.serializer.Serialize(userData.Username)
			require.NoError(t, err)
			require.NotNil(t, secondaryIndexBs)

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

			// search by key
			searchOpts := &IndexOpts{
				HasIdx:    true,
				ParentKey: item.Key,
			}
			bsItem, err := ltngEngine.LoadItem(ctx, databaseMetaInfo, item, searchOpts)
			assert.NoError(t, err)
			assert.NotNil(t, bsItem)

			// search by index
			searchOpts = &IndexOpts{
				HasIdx:       true,
				ParentKey:    item.Key,
				IndexingKeys: [][]byte{secondaryIndexBs},
			}
			bsItem, err = ltngEngine.LoadItem(ctx, databaseMetaInfo, item, searchOpts)
			assert.NoError(t, err)
			assert.NotNil(t, bsItem)
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
