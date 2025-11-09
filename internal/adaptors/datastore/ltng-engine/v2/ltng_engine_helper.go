package v2

import (
	"context"
	"runtime"
	"sync"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/serializer"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"

	filequeuev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/file_queue/v1"
	memorystorev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/memorystore/v1"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/rw"
)

func newLTNGEngine(
	ctx context.Context, opts ...options.Option,
) (*LTNGEngine, error) {
	fq, err := filequeuev1.New(ctx,
		filequeuev1.GenericFileQueueFilePath,
		filequeuev1.GenericFileQueueFileName,
	)
	if err != nil {
		return nil, err
	}

	engine := &LTNGEngine{
		ctx:                    ctx,
		kvLock:                 syncx.NewKVLock(),
		mtx:                    new(sync.RWMutex),
		fq:                     fq,
		fileManager:            rw.NewFileManager(ctx),
		memoryStore:            memorystorev1.New(ctx),
		storeFileMapping:       syncx.NewGenericMap[*ltngdata.FileInfo](),
		itemFileMapping:        syncx.NewGenericMap[*ltngdata.FileInfo](),
		markedAsDeletedMapping: syncx.NewGenericMap[struct{}](),
		serializer:             serializer.NewRawBinarySerializer(),
		logger:                 slogx.New(),
		mngrStoreInfo:          ltngdata.DBManagerStoreInfo,
	}
	options.ApplyOptions(engine, opts...)

	if err = engine.init(ctx); err != nil {
		return nil, err
	}

	return engine, nil
}

func (e *LTNGEngine) init(ctx context.Context) error {
	if err := e.createStatsPathsOnDisk(ctx, e.mngrStoreInfo); err != nil {
		return errorsx.Wrap(err, "failed initializing file queue")
	}

	if err := e.createDataPathsOnDisk(ctx, e.mngrStoreInfo); err != nil {
		return errorsx.Wrap(err, "failed initializing file queue")
	}

	if _, err := e.createOpenRelationalStatsStoreOnDisk(ctx); err != nil {
		return err
	}

	if err := e.fq.Init(); err != nil {
		return errorsx.Wrap(err, "failed initializing file queue")
	}

	e.opSaga = newOpSaga(ctx, e)

	return nil
}

func (e *LTNGEngine) close() {
	e.closeItems()
	e.closeStores()
}

func (e *LTNGEngine) closeStores() {
	e.storeFileMapping.RangeAndDelete(
		func(fileStore string, value *ltngdata.FileInfo) bool {
			if !rw.IsFileClosed(value.File) {
				if err := value.File.Close(); err != nil {
					e.logger.Error(e.ctx, "error closing file from store",
						"file_store", fileStore, "err", err)
				}
			}

			return true
		})
}

func (e *LTNGEngine) closeItems() {
	for !e.fq.CheckAndClose() {
		runtime.Gosched()
	}

	for e.opSaga.pidRegister.CountNumber() != 0 {
		runtime.Gosched()
	}

	e.itemFileMapping.RangeAndDelete(func(fileItem string, value *ltngdata.FileInfo) bool {
		if !rw.IsFileClosed(value.File) {
			if err := value.File.Close(); err != nil {
				e.logger.Error(e.ctx, "error closing file from item store",
					"file_item", fileItem, "err", err)
			}
		}

		return true
	})

	e.opSaga.Close()
}
