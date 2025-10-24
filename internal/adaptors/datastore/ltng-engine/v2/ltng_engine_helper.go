package v2

import (
	"context"
	"os"
	"runtime"
	"sync"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/serializer"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"

	filequeuev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/file_queue/v1"
	memorystorev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/memorystore/v1"
	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/lock"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/rw"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/safe"
)

func newLTNGEngine(
	ctx context.Context, opts ...options.Option,
) (*LTNGEngine, error) {
	logger := slogx.New()

	fq, err := filequeuev1.New(ctx,
		filequeuev1.GenericFileQueueFilePath,
		filequeuev1.GenericFileQueueFileName,
	)
	if err != nil {
		return nil, err
	}

	engine := &LTNGEngine{
		ctx:                    ctx,
		opMtx:                  lock.NewEngineLock(),
		mtx:                    new(sync.RWMutex),
		fq:                     fq,
		fileManager:            rw.NewFileManager(ctx),
		memoryStore:            memorystorev1.New(ctx),
		storeFileMapping:       safe.NewGenericMap[*ltngenginemodels.FileInfo](),
		itemFileMapping:        safe.NewGenericMap[*ltngenginemodels.FileInfo](),
		markedAsDeletedMapping: safe.NewGenericMap[struct{}](),
		serializer:             serializer.NewRawBinarySerializer(),
		logger:                 logger,
	}
	options.ApplyOptions(engine, opts...)

	if err = engine.init(ctx); err != nil {
		return nil, err
	}

	//engine.opSaga = newOpSaga(ctx, engine)
	return engine, nil
}

func (e *LTNGEngine) init(ctx context.Context) error {
	if err := e.createStatsPathOnDisk(ctx); err != nil {
		return err
	}
	if _, err := e.createOrOpenRelationalStatsStoreOnDisk(ctx); err != nil {
		return errorsx.Wrap(err, "failed creating store stats manager on-disk")
	}
	if err := os.MkdirAll(
		ltngenginemodels.DBTmpDelDataPath, ltngenginemodels.DBFilePerm,
	); err != nil {
		return err
	}
	if err := os.MkdirAll(
		ltngenginemodels.DBTmpDelStatsPath, ltngenginemodels.DBFilePerm,
	); err != nil {
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
		func(fileStore string, value *ltngenginemodels.FileInfo) bool {
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

	e.itemFileMapping.RangeAndDelete(func(fileItem string, value *ltngenginemodels.FileInfo) bool {
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
