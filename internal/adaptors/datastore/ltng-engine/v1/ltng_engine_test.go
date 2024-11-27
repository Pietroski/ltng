package v1

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ltng_engine_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/ltng-engine/v1"
)

func TestLTNGEngineFlow(t *testing.T) {
	t.Run("create - get - delete flow for single store", func(t *testing.T) {
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

	t.Run("create - get - delete flow for multiple stores", func(t *testing.T) {
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
