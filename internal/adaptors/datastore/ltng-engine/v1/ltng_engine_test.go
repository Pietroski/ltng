package v1

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLTNGEngine_CreateStore(t *testing.T) {
	t.Run("fail to get store", func(t *testing.T) {
		ctx := context.Background()
		ltngEngine := New()

		info, err := ltngEngine.CreateStore(ctx, &DBInfo{
			Name: "inexistent-test-store",
		})
		t.Log(err)
		assert.Error(t, err)
		assert.Nil(t, info)
	})
}

func TestLTNGEngine_LoadStore(t *testing.T) {
	t.Run("fail to get store", func(t *testing.T) {
		ctx := context.Background()
		ltngEngine := New()

		info, err := ltngEngine.LoadStore(ctx, &DBInfo{
			Name: "inexistent-test-store",
		})
		t.Log(err)
		assert.Error(t, err)
		assert.Nil(t, info)
	})
}
