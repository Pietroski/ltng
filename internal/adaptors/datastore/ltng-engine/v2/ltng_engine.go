package v2

import (
	"context"
	"sync"

	"gitlab.com/pietroski-software-company/golang/devex/options"
	serializer_models "gitlab.com/pietroski-software-company/golang/devex/serializer/models"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"

	filequeuev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/file_queue/v1"
	memorystorev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/memorystore/v1"
	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/rw"
)

type (
	LTNGEngine struct {
		ctx context.Context

		kvLock      *syncx.KVLock
		mtx         *sync.RWMutex
		opSaga      *opSaga
		serializer  serializer_models.Serializer
		memoryStore *memorystorev1.LTNGCacheEngine
		fileManager *rw.FileManager
		fq          *filequeuev1.FileQueue

		storeFileMapping       *syncx.GenericMap[*ltngenginemodels.FileInfo]
		itemFileMapping        *syncx.GenericMap[*ltngenginemodels.FileInfo]
		markedAsDeletedMapping *syncx.GenericMap[struct{}]

		logger slogx.SLogger
	}
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
	info *ltngenginemodels.StoreInfo,
) (store *ltngenginemodels.StoreInfo, err error) {
	return e.createStore(ctx, info)
}

func (e *LTNGEngine) LoadStore(
	ctx context.Context,
	info *ltngenginemodels.StoreInfo,
) (*ltngenginemodels.StoreInfo, error) {
	return e.loadStore(ctx, info)
}

func (e *LTNGEngine) DeleteStore(
	ctx context.Context,
	info *ltngenginemodels.StoreInfo,
) error {
	return e.deleteStore(ctx, info)
}

func (e *LTNGEngine) ListStores(
	ctx context.Context,
	pagination *ltngenginemodels.Pagination,
) ([]*ltngenginemodels.StoreInfo, error) {
	return e.listStores(ctx, pagination)
}

func (e *LTNGEngine) LoadItem(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	return e.loadItem(ctx, dbMetaInfo, item, opts)
}

func (e *LTNGEngine) CreateItem(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	return e.createItem(ctx, dbMetaInfo, item, opts)
}

func (e *LTNGEngine) UpsertItem(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	return e.upsertItem(ctx, dbMetaInfo, item, opts)
}

func (e *LTNGEngine) DeleteItem(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	return e.deleteItem(ctx, dbMetaInfo, item, opts)
}

func (e *LTNGEngine) ListItems(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	pagination *ltngenginemodels.Pagination,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.ListItemsResult, error) {
	return e.listItems(ctx, dbMetaInfo, pagination, opts)
}
