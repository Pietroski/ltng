package v3

import (
	"context"
	"sync"

	"gitlab.com/pietroski-software-company/golang/devex/options"
	serializermodels "gitlab.com/pietroski-software-company/golang/devex/serializer/models"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"
	"gitlab.com/pietroski-software-company/golang/devex/tracer"

	memorystorev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/memorystore/v1"
	v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdb/v3"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/mmap"
)

type (
	LTNGEngine struct {
		ctx      context.Context
		cancel   context.CancelFunc
		fqCtx    context.Context
		cancelFq context.CancelFunc

		kvLock *syncx.KVLock
		mtx    *sync.RWMutex
		opSaga *opSaga

		serializer  serializermodels.Serializer
		memoryStore *memorystorev1.LTNGCacheEngine

		fq                         *mmap.FileQueue
		storeFileMapping           *syncx.GenericMap[*v4.FileInfo]
		itemFileMapping            *syncx.GenericMap[*v4.FileInfo]
		relationalStoreFileMapping *syncx.GenericMap[*v4.RelationalFileInfo]
		relationalItemFileMapping  *syncx.GenericMap[*v4.RelationalFileInfo]
		markedAsDeletedMapping     *syncx.GenericMap[struct{}]

		logger slogx.SLogger
		tracer tracer.Tracer

		mngrStoreInfo *v4.StoreInfo
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
	info *v4.StoreInfo,
) (store *v4.StoreInfo, err error) {
	return e.createStore(ctx, info)
}

func (e *LTNGEngine) LoadStore(
	ctx context.Context,
	info *v4.StoreInfo,
) (*v4.StoreInfo, error) {
	return e.loadStore(ctx, info)
}

func (e *LTNGEngine) DeleteStore(
	ctx context.Context,
	info *v4.StoreInfo,
) error {
	return e.deleteStore(ctx, info)
}

func (e *LTNGEngine) ListStores(
	ctx context.Context,
	pagination *ltngdata.Pagination,
) ([]*v4.StoreInfo, error) {
	return e.listStores(ctx, pagination)
}

func (e *LTNGEngine) LoadItem(
	ctx context.Context,
	dbMetaInfo *v4.ManagerStoreMetaInfo,
	item *v4.Item,
	opts *v4.IndexOpts,
) (*v4.Item, error) {
	return e.loadItem(ctx, dbMetaInfo, item, opts)
}

func (e *LTNGEngine) CreateItem(
	ctx context.Context,
	dbMetaInfo *v4.ManagerStoreMetaInfo,
	item *v4.Item,
	opts *v4.IndexOpts,
) (*v4.Item, error) {
	return e.createItem(ctx, dbMetaInfo, item, opts)
}

func (e *LTNGEngine) UpsertItem(
	ctx context.Context,
	dbMetaInfo *v4.ManagerStoreMetaInfo,
	item *v4.Item,
	opts *v4.IndexOpts,
) (*v4.Item, error) {
	return e.upsertItem(ctx, dbMetaInfo, item, opts)
}

func (e *LTNGEngine) DeleteItem(
	ctx context.Context,
	dbMetaInfo *v4.ManagerStoreMetaInfo,
	item *v4.Item,
	opts *v4.IndexOpts,
) (*v4.Item, error) {
	return e.deleteItem(ctx, dbMetaInfo, item, opts)
}

func (e *LTNGEngine) ListItems(
	ctx context.Context,
	dbMetaInfo *v4.ManagerStoreMetaInfo,
	pagination *ltngdata.Pagination,
	opts *v4.IndexOpts,
) (*v4.ListItemsResult, error) {
	return e.listItems(ctx, dbMetaInfo, pagination, opts)
}
