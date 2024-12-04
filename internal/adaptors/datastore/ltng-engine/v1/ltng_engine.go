package v1

import (
	"context"

	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	ltng_engine_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/ltng-engine/v1"
)

func New(ctx context.Context, opts ...options.Option) (*LTNGEngine, error) {
	return newLTNGEngine(ctx, opts...)
}

func (e *LTNGEngine) Close() {
	e.close()
}

func (e *LTNGEngine) Restart(ctx context.Context) error {
	e.close()
	return e.init(ctx)
}

func (e *LTNGEngine) CreateStore(
	ctx context.Context,
	info *StoreInfo,
) (store *StoreInfo, err error) {
	return e.createStore(ctx, info)
}

func (e *LTNGEngine) LoadStore(
	ctx context.Context,
	info *StoreInfo,
) (*StoreInfo, error) {
	return e.loadStore(ctx, info)
}

func (e *LTNGEngine) DeleteStore(
	ctx context.Context,
	info *StoreInfo,
) error {
	return e.deleteStore(ctx, info)
}

func (e *LTNGEngine) ListStores(
	ctx context.Context,
	pagination *ltng_engine_models.Pagination,
) ([]*StoreInfo, error) {
	return e.listStores(ctx, pagination)
}

func (e *LTNGEngine) LoadItem(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
	opts *IndexOpts,
) ([]byte, error) {
	return e.loadItem(ctx, dbMetaInfo, item, opts)
}

func (e *LTNGEngine) CreateItem(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
	opts *IndexOpts,
) ([]byte, error) {
	return e.createItem(ctx, dbMetaInfo, item, opts)
}

func (e *LTNGEngine) UpsertItem(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
	opts *IndexOpts,
) ([]byte, error) {
	return e.createItem(ctx, dbMetaInfo, item, opts)
}

func (e *LTNGEngine) DeleteItem(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
	opts *IndexOpts,
) ([]byte, error) {
	return e.createItem(ctx, dbMetaInfo, item, opts)
}

func (e *LTNGEngine) ListItems(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
	opts *IndexOpts,
) ([][]byte, error) {
	return [][]byte{}, nil
}
