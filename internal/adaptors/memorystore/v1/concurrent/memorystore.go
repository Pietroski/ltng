package concurrentv1

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/lock"
	go_cache "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/cache"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
)

type LTNGCacheEngine struct {
	opMtx         *lock.EngineLock
	crudChannels  *ltngenginemodels.CrudChannels
	createChannel chan *ltngenginemodels.ItemInfoData

	itemFileMapping map[string]*ltngenginemodels.FileData

	cache go_cache.Cacher
}

func New(ctx context.Context, opts ...options.Option) *LTNGCacheEngine {
	engine := &LTNGCacheEngine{
		opMtx:           lock.NewEngineLock(),
		cache:           go_cache.New(),
		itemFileMapping: make(map[string]*ltngenginemodels.FileData),

		crudChannels: &ltngenginemodels.CrudChannels{
			CreateChannels: ltngenginemodels.MakeOpChannels(),
			UpsertChannels: ltngenginemodels.MakeOpChannels(),
			DeleteChannels: ltngenginemodels.MakeOpChannels(),
		},
		createChannel: make(chan *ltngenginemodels.ItemInfoData, 1<<8),
	}
	options.ApplyOptions(engine, opts...)

	newCreateSaga(ctx, engine)

	return engine
}

func (engine *LTNGCacheEngine) CreateItem(
	_ context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	respSignal := make(chan error)
	itemInfoData := &ltngenginemodels.ItemInfoData{
		DBMetaInfo: dbMetaInfo,
		Item:       item,
		Opts:       opts,
		RespSignal: respSignal,
	}
	engine.createChannel <- itemInfoData
	if err := <-respSignal; err != nil {
		return nil, err
	}

	return nil, nil
}

func (engine *LTNGCacheEngine) LoadItem(
	_ context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	if opts == nil {
		return nil, nil
	}

	key := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.Name), item.Key},
		[]byte(ltngenginemodels.BytesSep),
	)
	strKey := hex.EncodeToString(key)
	if !opts.HasIdx {
		engine.opMtx.Lock(dbMetaInfo.LockName(strKey), struct{}{})
		defer engine.opMtx.Unlock(dbMetaInfo.LockName(strKey))

		fd, ok := engine.itemFileMapping[strKey]
		if !ok {
			return nil, fmt.Errorf("not found")
		}

		return &ltngenginemodels.Item{
			Key:   fd.Key,
			Value: fd.Data,
		}, nil
	}

	return nil, fmt.Errorf("not found")
}
