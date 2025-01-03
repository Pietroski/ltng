package concurrentmemorystorev1

import (
	"context"

	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/lock"
	go_cache "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/cache"
)

type LTNGCacheEngine struct {
	opMtx *lock.EngineLock

	opSaga *opSaga
	cache  go_cache.Cacher
}

func New(ctx context.Context, opts ...options.Option) *LTNGCacheEngine {
	engine := &LTNGCacheEngine{
		opMtx: lock.NewEngineLock(),
		cache: go_cache.New(),
	}
	options.ApplyOptions(engine, opts...)

	op := newOpSaga(ctx, engine)
	engine.opSaga = op

	return engine
}

func WithCache(cache go_cache.Cacher) options.Option {
	return func(i interface{}) {
		if cfg, ok := i.(*LTNGCacheEngine); ok {
			cfg.cache = cache
		}
	}
}

func (engine *LTNGCacheEngine) CreateItem(
	_ context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	respSignal := make(chan error)
	itemInfoData := &ltngenginemodels.ItemInfoData{
		OpType:     ltngenginemodels.OpTypeCreate,
		DBMetaInfo: dbMetaInfo,
		Item:       item,
		Opts:       opts,
		RespSignal: respSignal,
	}
	engine.opSaga.crudChannels.OpSagaChannel.InfoChannel <- itemInfoData
	if err := <-respSignal; err != nil {
		return nil, err
	}

	return nil, nil
}
