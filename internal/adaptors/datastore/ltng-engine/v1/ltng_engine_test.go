package v1

import (
	"context"
	"github.com/stretchr/testify/require"
	ltng_engine_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/ltng-engine/v1"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLTNGEngine_CreateStore(t *testing.T) {
	t.Run("fail to create store", func(t *testing.T) {
		t.Run("missing path", func(t *testing.T) {
			ctx := context.Background()
			ltngEngine, err := New(ctx)
			require.NoError(t, err)

			info, err := ltngEngine.CreateStore(ctx, &DBInfo{
				Name: "inexistent-test-store",
			})
			assert.Error(t, err)
			assert.Nil(t, info)
			t.Log(err)
		})
	})

	t.Run("create store successfully", func(t *testing.T) {
		ctx := context.Background()
		ltngEngine, err := New(ctx)
		require.NoError(t, err)

		info, err := ltngEngine.CreateStore(ctx, &DBInfo{
			Name: "test-store",
			Path: "test-path",
		})
		assert.NoError(t, err)

		t.Log(info)
	})
}

func TestLTNGEngine_LoadStore(t *testing.T) {
	t.Run("fail to get store", func(t *testing.T) {
		ctx := context.Background()
		ltngEngine, err := New(ctx)
		require.NoError(t, err)

		info, err := ltngEngine.LoadStore(ctx, &DBInfo{
			Name: "inexistent-test-store",
		})
		assert.Error(t, err)
		assert.Nil(t, info)
		t.Log(err)
	})

	t.Run("fail to get store", func(t *testing.T) {
		ctx := context.Background()
		ltngEngine, err := New(ctx)
		require.NoError(t, err)

		info, err := ltngEngine.LoadStore(ctx, &DBInfo{
			Name: "test-store",
			Path: "test-path",
		})
		assert.NoError(t, err)
		assert.NotNil(t, info)
		t.Log(info)
	})
}

func TestLTNGEngine_DeleteStore(t *testing.T) {
	t.Run("fail to delete store", func(t *testing.T) {
		ctx := context.Background()
		ltngEngine, err := New(ctx)
		require.NoError(t, err)

		info, err := ltngEngine.CreateStore(ctx, &DBInfo{
			Name: "test-store",
			Path: "test-path",
		})
		assert.NoError(t, err)
		t.Log(info)

		info, err = ltngEngine.LoadStore(ctx, &DBInfo{
			Name: "test-store",
			Path: "test-path",
		})
		assert.NoError(t, err)
		t.Log(info)

		err = ltngEngine.DeleteStore(ctx, &DBInfo{
			Name: "test-store",
			Path: "test-path",
		})
		assert.NoError(t, err)

		info, err = ltngEngine.LoadStore(ctx, &DBInfo{
			Name: "test-store",
			Path: "test-path",
		})
		assert.Error(t, err)
		assert.Nil(t, info)
	})
}

func TestLTNGEngineFlow(t *testing.T) {
	t.Run("create - get - delete flow", func(t *testing.T) {
		ctx := context.Background()
		ltngEngine, err := New(ctx)
		require.NoError(t, err)

		info, err := ltngEngine.CreateStore(ctx, &DBInfo{
			Name: "test-store",
			Path: "test-path",
		})
		assert.NoError(t, err)
		assert.NotNil(t, info)

		info, err = ltngEngine.LoadStore(ctx, &DBInfo{
			Name: "test-store",
			Path: "test-path",
		})
		assert.NoError(t, err)

		err = ltngEngine.DeleteStore(ctx, &DBInfo{
			Name: "test-store",
			Path: "test-path",
		})
		assert.NoError(t, err)

		info, err = ltngEngine.LoadStore(ctx, &DBInfo{
			Name: "test-store",
			Path: "test-path",
		})
		assert.Error(t, err)
		assert.Nil(t, info)
	})

	t.Run("detect last opened at difference after closing", func(t *testing.T) {
		ctx := context.Background()
		ltngEngine, err := New(ctx)
		require.NoError(t, err)
		infoHistory := make([]*DBInfo, 0)

		info, err := ltngEngine.CreateStore(ctx, &DBInfo{
			Name: "test-store",
			Path: "test-path",
		})
		assert.NoError(t, err)
		infoHistory = append(infoHistory, info)

		info, err = ltngEngine.LoadStore(ctx, &DBInfo{
			Name: "test-store",
			Path: "test-path",
		})
		assert.NoError(t, err)
		infoHistory = append(infoHistory, info)

		ltngEngine.Close()
		time.Sleep(1 * time.Second)

		info, err = ltngEngine.LoadStore(ctx, &DBInfo{
			Name: "test-store",
			Path: "test-path",
		})
		assert.NoError(t, err)
		infoHistory = append(infoHistory, info)

		err = ltngEngine.DeleteStore(ctx, &DBInfo{
			Name: "test-store",
			Path: "test-path",
		})
		assert.NoError(t, err)

		info, err = ltngEngine.LoadStore(ctx, &DBInfo{
			Name: "test-store",
			Path: "test-path",
		})
		assert.Error(t, err)
		assert.Nil(t, info)
		infoHistory = append(infoHistory, info)

		assertLOFromHistory(t, infoHistory)
	})

	t.Run("create - get - delete flow", func(t *testing.T) {
		ctx := context.Background()
		ltngEngine, err := New(ctx)
		require.NoError(t, err)

		info, err := ltngEngine.CreateStore(ctx, &DBInfo{
			Name: "test-store",
			Path: "test-path",
		})
		assert.NoError(t, err)
		assert.NotNil(t, info)

		info, err = ltngEngine.CreateStore(ctx, &DBInfo{
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

		err = ltngEngine.DeleteStore(ctx, &DBInfo{
			Name: "test-store",
			Path: "test-path",
		})
		assert.NoError(t, err)

		err = ltngEngine.DeleteStore(ctx, &DBInfo{
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

		info, err = ltngEngine.LoadStore(ctx, dbManagerInfo.RelationalInfo())
		assert.NoError(t, err)
		assert.NotNil(t, info)
		t.Log(info)
	})
}

func assertLOFromHistory(t *testing.T, history []*DBInfo) {
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
