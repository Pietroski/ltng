package ltngdbenginev3

import (
	"context"
	"sync"

	"gitlab.com/pietroski-software-company/golang/devex/options"
	serializermodels "gitlab.com/pietroski-software-company/golang/devex/serializer/models"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"
	"gitlab.com/pietroski-software-company/golang/devex/tracer"

	memorystorev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/memorystore/v1"
	ltngdbenginemodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
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
		storeFileMapping           *syncx.GenericMap[*ltngdbenginemodelsv3.FileInfo]
		itemFileMapping            *syncx.GenericMap[*ltngdbenginemodelsv3.FileInfo]
		relationalStoreFileMapping *syncx.GenericMap[*ltngdbenginemodelsv3.RelationalFileInfo]
		relationalItemFileMapping  *syncx.GenericMap[*ltngdbenginemodelsv3.RelationalFileInfo]
		markedAsDeletedMapping     *syncx.GenericMap[struct{}]

		logger slogx.SLogger
		tracer tracer.Tracer

		mngrStoreInfo *ltngdbenginemodelsv3.StoreInfo
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
	info *ltngdbenginemodelsv3.StoreInfo,
) (store *ltngdbenginemodelsv3.StoreInfo, err error) {
	return e.createStore(ctx, info)
}

func (e *LTNGEngine) LoadStore(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) (*ltngdbenginemodelsv3.StoreInfo, error) {
	return e.loadStore(ctx, info)
}

func (e *LTNGEngine) DeleteStore(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) error {
	return e.deleteStore(ctx, info)
}

func (e *LTNGEngine) ListStores(
	ctx context.Context,
	pagination *ltngdata.Pagination,
) ([]*ltngdbenginemodelsv3.StoreInfo, error) {
	return e.listStores(ctx, pagination)
}

func (e *LTNGEngine) LoadItem(
	ctx context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
	item *ltngdbenginemodelsv3.Item,
	opts *ltngdbenginemodelsv3.IndexOpts,
) (*ltngdbenginemodelsv3.Item, error) {
	return e.loadItem(ctx, dbMetaInfo, item, opts)
}

func (e *LTNGEngine) CreateItem(
	ctx context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
	item *ltngdbenginemodelsv3.Item,
	opts *ltngdbenginemodelsv3.IndexOpts,
) (*ltngdbenginemodelsv3.Item, error) {
	return e.createItem(ctx, dbMetaInfo, item, opts)
}

func (e *LTNGEngine) UpsertItem(
	ctx context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
	item *ltngdbenginemodelsv3.Item,
	opts *ltngdbenginemodelsv3.IndexOpts,
) (*ltngdbenginemodelsv3.Item, error) {
	return e.upsertItem(ctx, dbMetaInfo, item, opts)
}

func (e *LTNGEngine) DeleteItem(
	ctx context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
	item *ltngdbenginemodelsv3.Item,
	opts *ltngdbenginemodelsv3.IndexOpts,
) (*ltngdbenginemodelsv3.Item, error) {
	return e.deleteItem(ctx, dbMetaInfo, item, opts)
}

func (e *LTNGEngine) ListItems(
	ctx context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
	pagination *ltngdata.Pagination,
	opts *ltngdbenginemodelsv3.IndexOpts,
) (*ltngdbenginemodelsv3.ListItemsResult, error) {
	return e.listItems(ctx, dbMetaInfo, pagination, opts)
}
