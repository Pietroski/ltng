package memorystorev1

import (
	"context"

	"gitlab.com/pietroski-software-company/golang/devex/options"

	ltngdata "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdb/v3"
	pagination "gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	go_cache "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/cache"
)

type LTNGCacheEngine struct {
	cache go_cache.Cacher
	//cm    *concurrentmemorystorev1.LTNGCacheEngine
}

func New(ctx context.Context, opts ...options.Option) *LTNGCacheEngine {
	cache := &LTNGCacheEngine{
		cache: go_cache.New(),
	}
	options.ApplyOptions(cache, opts...)

	//cache.cm = concurrentmemorystorev1.New(ctx,
	//	concurrentmemorystorev1.WithCache(cache.cache))

	return cache
}

func (ltng *LTNGCacheEngine) CreateItem(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	//return ltng.cm.CreateItem(ctx, dbMetaInfo, item, opts)
	return ltng.createItem(ctx, dbMetaInfo, item, opts)
}

func (ltng *LTNGCacheEngine) UpsertItem(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	return ltng.upsertItem(ctx, dbMetaInfo, item, opts)
}

func (ltng *LTNGCacheEngine) DeleteItem(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	return ltng.deleteItem(ctx, dbMetaInfo, item, opts)
}

func (ltng *LTNGCacheEngine) LoadItem(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	return ltng.loadItem(ctx, dbMetaInfo, item, opts)
}

func (ltng *LTNGCacheEngine) ListItems(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	pagination *pagination.Pagination,
	opts *ltngdata.IndexOpts,
) (*ltngdata.ListItemsResult, error) {
	return ltng.listItems(ctx, dbMetaInfo, pagination, opts)
}
